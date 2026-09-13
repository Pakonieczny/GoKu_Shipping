const assert=require('node:assert/strict');
const {orderAssetGroupMutations}=require('../../netlify/functions/googleAdsAdDesign');
const group='customers/123/assetGroups/7',ops=[];
for(const [field,oldCount,newCount] of [['HEADLINE',10,5],['LONG_HEADLINE',2,2],['DESCRIPTION',4,3]]){
 for(let i=0;i<oldCount;i++)ops.push({assetGroupAssetOperation:{remove:'customers/123/assetGroupAssets/7~'+(100+i)+'~'+field}});
 for(let i=0;i<newCount;i++){const asset='customers/123/assets/'+(-1000-ops.length);ops.push({assetOperation:{create:{resourceName:asset,textAsset:{text:field+' '+i}}}},{assetGroupAssetOperation:{create:{assetGroup:group,asset,fieldType:field}}});}
}
const before=JSON.stringify(ops),ordered=orderAssetGroupMutations(ops),firstLink=ordered.findIndex(o=>o.assetGroupAssetOperation);
assert.equal(JSON.stringify(ops),before,'reviewed payload not mutated');
assert.equal(ordered.length,ops.length,'no approved operation dropped');
assert(ordered.slice(0,firstLink).every(o=>o.assetOperation),'all immutable assets created first');
assert(ordered.slice(firstLink).every(o=>o.assetGroupAssetOperation),'all removals and additions stay in one contiguous group');
assert.deepEqual(ordered.filter(o=>o.assetGroupAssetOperation),ops.filter(o=>o.assetGroupAssetOperation),'approved link operations retain relative order');
const other={assetGroupAssetOperation:{remove:'customers/123/assetGroupAssets/8~999~HEADLINE'}},deletion={assetGroupOperation:{remove:'customers/123/assetGroups/8'}};
const mixed=orderAssetGroupMutations([other,deletion,...ops]);assert(mixed.indexOf(other)<mixed.indexOf(deletion),'unrelated deletion ordering preserved');
const second={assetGroupAssetOperation:{create:{assetGroup:'customers/123/assetGroups/9',asset:'customers/123/assets/12',fieldType:'HEADLINE'}}};
const groups=orderAssetGroupMutations([ops[0],second,...ops.slice(1)]).filter(o=>o.assetGroupAssetOperation);assert.equal(groups.at(-1),second,'separate group batch stays separate');
console.log('PASS 7 replacement mutation grouping and approved-payload preservation checks');
