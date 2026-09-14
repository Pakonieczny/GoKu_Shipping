// Campaign-specific routing. No network, implicit selection, or budget multiplication.
const STYLES = ['fixed_display', 'responsive_display', 'pmax'];
const NAMES = {fixed_display:'Fixed Display',responsive_display:'Responsive Display',pmax:'Performance Max'};
const FIXED_SIZES = new Set(['200x200','240x400','250x250','250x360','300x250','336x280','580x400','120x600','160x600','300x600','300x1050','468x60','728x90','930x180','970x90','970x250','980x120','300x50','320x50','320x100']);
function selection(styles,budgets,countries){
  if(!Array.isArray(styles)||!styles.length||styles.length>3||new Set(styles).size!==styles.length||styles.some(s=>!STYLES.includes(s)))throw Error('Choose one, two or all three campaign styles.');
  const chosen=STYLES.filter(s=>styles.includes(s)),daily={};
  for(const s of chosen){const n=Number(budgets?.[s]);if(!Number.isFinite(n)||n<=0||n>100000||Math.abs(Math.round(n*100)-n*100)>1e-7)throw Error('Enter a positive daily budget with at most two decimal places for '+NAMES[s]+'.');daily[s]=n;}
  if(!Array.isArray(countries)||!countries.length||countries.some(c=>!/^\d{4,10}$/.test(String(c))))throw Error('Choose the target country IDs before preparing these campaigns.');
  return {styles:chosen,budgets:daily,countries:[...new Set(countries.map(String))].sort(),totalDaily:Math.round(Object.values(daily).reduce((a,b)=>a+b,0)*100)/100};
}
function fixedProofs(images){
  const found=new Map();
  for(const p of images||[]){const size=p.width+'x'+p.height;if(FIXED_SIZES.has(size)&&p.asset&&!found.has(size))found.set(size,p);}
  if(!found.size)throw Error('No Google-supported fixed-size proofs are saved. Save the complete ad review first.');
  return [...found.values()];
}
function validatePhoto(asset,shape){
  const spec={square:[1,300,300],landscape:[1.91,600,314],portrait:[.8,480,600],logo:[1,128,128]}[shape];
  if(!asset||!spec||Math.abs(asset.width/asset.height-spec[0])>spec[0]*.01||asset.width<spec[1]||asset.height<spec[2]||asset.bytes>5120*1024)throw Error('The '+shape+' photograph does not meet Google requirements. Save the correct crop first.');
}
function displayOps({customerId,style,name,dailyBudget,countries,destination,images,copy,logo,videos=[]}){
  if(!['fixed_display','responsive_display'].includes(style))throw Error('Invalid Display style.');
  const budget=`customers/${customerId}/campaignBudgets/-1`,campaign=`customers/${customerId}/campaigns/-2`,group=`customers/${customerId}/adGroups/-3`;
  const ops=[{campaignBudgetOperation:{create:{resourceName:budget,name:name+' budget',amountMicros:String(Math.round(dailyBudget*1e6)),deliveryMethod:'STANDARD',explicitlyShared:false}}},
    {campaignOperation:{create:{resourceName:campaign,name,status:'PAUSED',advertisingChannelType:'DISPLAY',campaignBudget:budget,maximizeConversions:{},containsEuPoliticalAdvertising:'DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING',geoTargetTypeSetting:{positiveGeoTargetType:'PRESENCE'}}}},
    ...countries.map(c=>({campaignCriterionOperation:{create:{campaign,location:{geoTargetConstant:'geoTargetConstants/'+c}}}})),
    {adGroupOperation:{create:{resourceName:group,campaign,name,type:'DISPLAY_STANDARD',status:'ENABLED'}}}];
  if(style==='fixed_display'){
    for(const image of images)ops.push({adGroupAdOperation:{create:{adGroup:group,status:'ENABLED',ad:{name:name+' '+image.width+'x'+image.height,finalUrls:[destination],imageAd:{imageAsset:{asset:image.resourceName}}}}}});
  }else{
    const text=(values,max,label)=>{if(!Array.isArray(values)||!values.length||values.some(t=>!t||[...t].length>max))throw Error('Review the '+label+' before publishing.');return values.slice(0,5).map(text=>({text}));};
    const headlines=text(copy.headlines,30,'headlines'),descriptions=text(copy.descriptions,90,'descriptions'),longHeadline=text(copy.longHeadlines,90,'long headline')[0];
    ops.push({adGroupAdOperation:{create:{adGroup:group,status:'ENABLED',ad:{name,finalUrls:[destination],responsiveDisplayAd:{headlines,descriptions,longHeadline,businessName:'Brites Jewelry',callToActionText:'Shop now',marketingImages:images.filter(i=>i.shape==='landscape').map(i=>({asset:i.resourceName})),squareMarketingImages:images.filter(i=>i.shape==='square').map(i=>({asset:i.resourceName})),squareLogoImages:[{asset:logo}],mainColor:'#f8eee1',accentColor:'#88452e',allowFlexibleColor:false,controlSpec:{enableAssetEnhancements:false,enableAutogenVideo:false},...(videos.length?{youtubeVideos:videos.map(asset=>({asset}))}:{})}}}}});
  }
  return ops;
}
module.exports={STYLES,NAMES,FIXED_SIZES,selection,fixedProofs,validatePhoto,displayOps};
