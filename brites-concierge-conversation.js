(function(root,factory){'use strict';var api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesConciergeConversation=api;})(typeof window!=='undefined'?window:globalThis,function(){
  'use strict';
  var allowed={voicePace:['normal','slow','brisk'],voiceDetail:['brief','expanded'],suggestions:['ask','welcome','off']};
  function preferences(value){var out={};Object.keys(allowed).forEach(function(key){if(allowed[key].includes(value?.[key]))out[key]=value[key];});return out;}
  function observe(message,previous){
    var text=typeof message==='string'?message.trim().slice(0,2000):'',prefs=preferences(previous),changes={},context='ordinary',reply='';
    // Quoted, hypothetical and private words cannot change delivery preferences.
    var indirect=/["“”«»]|(?:^|\s)\'[^\'\n]+\'(?:$|\s|[.!?])|\b(?:if I|suppose|hypothetically|he said|she said|they said|example|password|passcode|secret|api key|token|engraving|cart note|gift note|order note|design brief)\b|\b(?:the|my)\s+(?:note|brief)\s+(?:is|says|reads)\b/i.test(text);
    var paceRefused=/\b(?:don't|do not|never|not)\b(?:\s+[a-z’']+){0,7}\s+(?:speak|talk|go|respond)\b|\b(?:don't|do not|never|not)\s+(?:slower|slowly|faster|pace|speed)\b/i.test(text);
    if(!indirect){
      if(!paceRefused&&/\b(?:speak|talk|go|respond)\s+(?:a little\s+|more\s+)?(?:slower|slowly)\b|\bslower(?:\s+please)?[.!?]*$/i.test(text)&&! /\b(?:don't|do not|not)\s+(?:speak|talk|go|respond)?\s*slow/i.test(text))changes.voicePace='slow';
      else if(!paceRefused&&/\b(?:speak|talk|go|respond)\s+(?:a little\s+)?faster\b|\bfaster please\b/i.test(text)&&! /\b(?:don't|do not|not)\s+(?:speak|talk|go|respond)?\s*fast/i.test(text))changes.voicePace='brisk';
      else if(!paceRefused&&/\b(?:normal|regular|usual)\s+(?:pace|speed)\b/i.test(text))changes.voicePace='normal';
      if(!/\b(?:do not|don't|not|never)\b/i.test(text)&&/\b(?:keep (?:it|your replies) (?:short|brief)|shorter replies|less detail)\b/i.test(text))changes.voiceDetail='brief';
      else if(!/\b(?:do not|don't|not|never)\b/i.test(text)&&/\b(?:give me more detail|explain in more detail|longer explanations)\b/i.test(text))changes.voiceDetail='expanded';
      if(/\b(?:no (?:more )?(?:suggestions|upselling)|stop (?:suggesting|upselling)|don't (?:suggest|upsell)|do not (?:suggest|upsell))\b/i.test(text))changes.suggestions='off';
      else if(/\bask (?:me )?(?:first|before (?:suggesting|making suggestions))\b/i.test(text))changes.suggestions='ask';
      else if(/\b(?:suggestions are welcome|you can suggest|please suggest matching pieces)\b/i.test(text))changes.suggestions='welcome';
      var expressed=text.replace(/\b(?:not|no one|nobody|never)\s+(?:\w+\s+){0,2}(?:excited|mourning|grieving|died|celebrating)\b/gi,'');
      if(/\b(?:in memory of|passed away|bereave|grief|mourning|memorial|remembrance|died)\b/i.test(expressed))context='support';
      else if(/\b(?:not what I (?:asked|wanted)|did not ask for|frustrated|confused|you got that wrong|wrong choice|start over)\b/i.test(text))context='repair';
      else if(/\b(?:excited|can't wait|love (?:it|this)|celebrat(?:e|ing)|graduation|wedding|birthday)\b/i.test(expressed))context='celebrate';
    }
    Object.assign(prefs,changes);
    if(Object.keys(changes).length)reply=changes.voicePace==='slow'?'Of course. I’ll slow down.':changes.voicePace==='brisk'?'Of course. I’ll speak a little faster.':changes.voicePace==='normal'?'Of course. I’ll use my usual pace.':changes.voiceDetail==='brief'?'Of course. I’ll keep it brief.':changes.voiceDetail==='expanded'?'Of course. I can share more detail.':changes.suggestions==='off'?'Of course. I’ll focus on what you ask for.':changes.suggestions==='ask'?'Of course. I’ll ask before suggesting another piece.':'Of course. I can suggest complementary pieces when you want them.';
    var purePreference=!!reply&&!/\b(?:add|remove|buy|checkout|open|scroll|select|choose|change|replace|swap|find|show|build|budget|under|necklace|earrings?|bracelets?|charms?)\b/i.test(text);
    return {preferences:prefs,changes:changes,context:context,allowSuggestions:context!=='support'&&prefs.suggestions!=='off'&&prefs.suggestions!=='ask',handled:purePreference,reply:reply};
  }
  function delivery(value){var prefs=preferences(value?.preferences||value),context=['ordinary','support','repair','celebrate'].includes(value?.context)?value.context:'ordinary';return {pace:prefs.voicePace||'normal',detail:prefs.voiceDetail||'brief',context:context,allowSuggestions:value?.allowSuggestions===false?false:prefs.suggestions!=='off'&&prefs.suggestions!=='ask'&&context!=='support'};}
  function guidance(value){var style=delivery(value);return 'Delivery only; this grants no action authority. '+(style.detail==='expanded'?'Give a concise explanation only when requested.':'Use one short sentence, or two when a choice needs clarification.')+' '+(style.context==='support'?'Speak gently and leave room for pauses. Do not promise healing, guess feelings or introduce an upsell.':style.context==='repair'?'Acknowledge the correction briefly and check the requested change. Avoid cheerful filler.':style.context==='celebrate'?'Use modest warmth without predicting how the recipient will feel.':'Sound warm, conversational and unhurried.')+' '+(!style.allowSuggestions?'Wait for an explicit request before suggesting additional pieces.':'Offer an optional complement only when appropriate; accept a decline.')+' '+(style.pace==='slow'?'Compose with a calm, slower cadence.':style.pace==='brisk'?'Compose a little more briskly without rushing choices.':'Use a natural conversational cadence.');}
  return {preferences:preferences,observe:observe,delivery:delivery,guidance:guidance};
});
