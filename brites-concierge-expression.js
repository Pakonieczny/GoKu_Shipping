(function(scope){
  'use strict';
  // Presentation only. Text is never an emotion diagnosis or shopping authority.
  // Native WebRTC supplies buffer events, not word timestamps. This bounded
  // phrase clock is an estimate, gated by measured audible activity.
  var KINDS=Object.freeze(['neutral','attentive','inquiry','explain','emphasize','reflect','support','celebrate','appreciate','resolve']);
  var clamp=function(v,a,b){return Math.max(a,Math.min(b,Number.isFinite(v)?v:a));};
  var clean=function(t){return typeof t==='string'?t.replace(/[\u0000-\u001f]/g,' ').slice(0,8000):'';};
  function withoutNegatedLoss(text){return text.replace(/\b(?:not|isn't|isn’t|wasn't|wasn’t|no longer)\s+(?:for\s+)?(?:a\s+)?(?:sad|memorial|remembrance|bereavement)\b/g,'').replace(/\b(?:no one|nobody)\s+(?:died|passed away)\b/g,'').replace(/\b(?:didn't|didn’t|hasn't|hasn’t|not)\s+(?:died|die|passed away|grieving)\b/g,'');}
  function withoutNegatedRepair(text){return text.replace(/\b(?:not|isn't|isn’t|wasn't|wasn’t|never|no longer)\s+(?:feeling\s+)?(?:confused|frustrated|annoyed|overwhelmed|broken|wrong)\b/g,'');}
  function contextFor(text,prior){
    var t=clean(text).toLowerCase();
    var deniedQuiet=/\b(?:not|isn't|isn’t|wasn't|wasn’t|no longer)\s+(?:for\s+)?(?:a\s+)?(?:sad|memorial|remembrance|bereavement)\b/.test(t)||/\b(?:no one|nobody)\s+(?:died|passed away)\b/.test(t);
    var affirmed=withoutNegatedLoss(t);
    if(/\b(?:died|passed away|grieving|grief|bereavement|memorial|remembrance|loss of (?:my |her |his |a )?(?:mother|father|sister|brother|friend|partner|child|loved one)|lost (?:my |her |his )?(?:mother|father|mom|dad|sister|brother|friend|partner|child)|in memory of)\b/.test(affirmed))return 'support';
    var repaired=/\b(?:fixed it|working now|not broken anymore|no longer broken|all resolved)\b/.test(t);
    if(!repaired&&/\b(?:frustrat\w*|not working|doesn't work|doesn’t work|confus\w*|overwhelmed|wrong|broken|annoy\w*)\b/.test(withoutNegatedRepair(t)))return 'repair';
    if(deniedQuiet||repaired||/\b(?:switch topics|different topic|moving on|another gift)\b/.test(t))return 'ordinary';
    return prior==='support'||prior==='repair'?prior:'ordinary';
  }
  function intent(text,context,role){
    var t=withoutNegatedLoss(text.toLowerCase()),kind='explain',intensity=.55;
    var celebration=/\b(?:congratulations|congrats|happy birthday|celebrat\w*|wonderful news|excited|graduat\w*|anniversary)\b/.test(t);
    var negated=/\b(?:not|isn't|isn’t|wasn't|wasn’t|don't|don’t|do not|never|hate|terrible|awful|unhappy)\b/.test(t);
    if(/\b(?:died|passed away|grieving|grief|bereavement|memorial|remembrance|loss of (?:my |her |his |a )?(?:mother|father|sister|brother|friend|partner|child|loved one)|lost (?:my |her |his )?(?:mother|father|mom|dad|sister|brother|friend|partner|child)|in memory of)\b/.test(t)){kind='support';intensity=.38;}
    else if(/\b(?:thank you for nothing|yeah right|as if|are you kidding|seriously wrong)\b/.test(t)){kind='support';intensity=.35;}
    else if(/\?$/.test(t)||/^(?:what|which|how|would you|do you|could you|shall we|tell me)\b/.test(t)){kind='inquiry';intensity=.64;}
    else if(/\b(?:not sure|uncertain|might|may suggest|may feel|may be|could mean|perhaps|cannot confirm|can't confirm|can’t confirm|cannot say|can't say|can’t say|don't know|do not know|depends|interpretation|personally|too expensive|too high|over budget|for you)\b/.test(t)||negated&&celebration){kind='reflect';intensity=.5;}
    else if(/\b(?:thanks|thank you|appreciate|grateful)\b/.test(t)){kind='appreciate';intensity=.62;}
    else if(/\b(?:fixed it|working now|all resolved|confirmed|that is correct|that's right|that’s right|we can|here is|here's|here’s)\b/.test(t)){kind='resolve';intensity=.5;}
    else if(/\b(?:sorry|missed what|misunderstood|got that wrong|let me check|try again|fix|clarify|check that|one step at a time)\b/.test(t)){kind='support';intensity=.45;}
    else if(celebration){kind='celebrate';intensity=.72;}
    else if(/\b(?:important|especially|remember|notice|the key|take a look|for example)\b/.test(t)){kind='emphasize';intensity=.63;}
    else if(role==='user'){kind='attentive';intensity=.4;}
    // Quiet context constrains amplitude without replacing every speech act.
    if(context==='support'||context==='repair'){if(kind==='celebrate'||kind==='attentive')kind='support';intensity=Math.min(intensity,context==='support'?.38:.45);}
    return {kind:kind,intensity:intensity};
  }
  function plan(text,options){
    options=options||{};text=clean(text).trim();if(!text)return [];
    var role=options.role==='user'?'user':'assistant',context=options.context||'ordinary',phrases=[],offset=0,wordsAt=0;
    // Clauses and connective phrases, rather than isolated keywords or every word.
    var protectedText=text.replace(/\b(?:Dr|Mr|Mrs|Ms|Prof|vs|e\.g|i\.e)\./gi,function(v){return v.replace(/\./g,'\ue000');});
    var chunks=protectedText.split(/(?<=[.!?;])\s+|(?<=[!?;])(?=\S)/u).filter(Boolean).map(function(v){return v.replace(/\ue000/g,'.');});
    chunks.forEach(function(chunk){
      var parts=chunk.trim().split(/,\s+(?=(?:but|and|so|because|while|which|if|would|what|how)\b)|\s+(?=but\b)/i);
      parts.forEach(function(part){var signal=intent(part,context,role),words=part.trim().split(/\s+/).filter(Boolean);while(words.length){var segment=words.splice(0,words.length>14?9:words.length).join(' ');if(!segment)continue;
        var n=segment.split(/\s+/).length,duration=clamp(n*310+( /[.!?;]$/.test(segment)?180:80),650,5000);
        phrases.push({index:phrases.length,kind:signal.kind,intensity:signal.intensity,startMs:offset,durationMs:duration,startWord:wordsAt,wordCount:n});offset+=duration;wordsAt+=n;
      }});
    });return phrases.slice(0,24);
  }
  function envelope(elapsed,cue){
    if(!cue||!Number.isFinite(elapsed)||elapsed<0||elapsed>cue.durationMs)return 0;
    var rise=Math.min(380,cue.durationMs*.3),fall=Math.min(650,cue.durationMs*.4),x=clamp(elapsed/rise,0,1),y=clamp((cue.durationMs-elapsed)/fall,0,1);
    var ease=function(v){return v*v*(3-2*v);};return ease(x)*ease(y);
  }
  function create(options){
    options=options||{};var now=typeof options.now==='function'?options.now:function(){return Date.now();};
    var onExpression=typeof options.onExpression==='function'?options.onExpression:function(){},onEvent=typeof options.onEvent==='function'?options.onEvent:function(){};
    var active=false,paused=false,reduced=false,destroyed=false,listening=false,playing=false,turnId='',responseId='',itemId='',context='ordinary',assistantText='',userText='',phrases=[],clock=0,lastTick=now(),lastOutputAt=-Infinity,lastInputAt=-Infinity,inputRunAt=null,lastBackchannel=-Infinity,inputObservedMs=0,lastLevelAt=now(),listenCueAt=null,listenKind='attentive',output=0,input=0,index=-1,current=null,transitions=0,mediaAt=null,mediaPositive=false,mediaDriven=false,closedResponses=new Set();
    function retire(id){if(!id)return;closedResponses.add(id);if(closedResponses.size>24)closedResponses.delete(closedResponses.values().next().value);}
    function emit(kind,intensity){var value=kind?{kind:kind,intensity:clamp(intensity,0,1)}:null;if(current?.kind===value?.kind&&Math.abs((current?.intensity||0)-(value?.intensity||0))<.002)return;current=value;try{onExpression(value);}catch(e){}}
    function clear(){playing=false;listening=false;assistantText=userText='';phrases=[];clock=0;output=input=0;mediaAt=null;mediaPositive=mediaDriven=false;inputRunAt=null;inputObservedMs=0;lastLevelAt=now();listenCueAt=null;index=-1;lastOutputAt=lastInputAt=-Infinity;lastTick=now();emit(null,0);}
    function beginTurn(value){if(destroyed||paused||reduced)return false;value=value||{};if(responseId)retire(responseId);clear();active=true;turnId=clean(String(value.id||'')).slice(0,200);responseId=itemId='';context=['ordinary','support','repair'].includes(value.context)?value.context:contextFor(value.context||'',null);lastBackchannel=-Infinity;return true;}
    function transcript(value){
      if(!active||paused||reduced||destroyed||!value||value.currentTurn===false||value.inputItemId&&turnId&&value.inputItemId!==turnId)return false;
      if(value.role==='user'){
        if(value.itemId&&turnId&&value.itemId!==turnId)return false;
        userText=typeof value.text==='string'?clean(value.text):clean(userText+clean(value.delta));context=contextFor(userText,context);
        var signal=intent(userText,context,'user');listenKind=signal.kind;
        // Transcript may arrive only after a committed turn. Never pretend it was
        // understood earlier. No assent nod or automatic spoken backchannel.
        if(listening&&userText.trim()){listenCueAt=now();emit(listenKind,Math.min(.5,signal.intensity));}return true;
      }
      if(value.role!=='assistant')return false;
      var nextId=clean(value.responseId||value.itemId||'').slice(0,200);
      if(nextId&&closedResponses.has(nextId))return false;
      if(responseId&&nextId&&nextId!==responseId){if(playing)return false;assistantText='';phrases=[];clock=0;index=-1;}
      if(responseId&&nextId===responseId&&itemId&&value.itemId&&itemId!==value.itemId)return false;
      if(Number.isInteger(value.contentIndex)&&value.contentIndex!==0)return false;
      if(nextId)responseId=nextId;if(value.itemId)itemId=clean(value.itemId).slice(0,200);
      assistantText=typeof value.text==='string'?clean(value.text):clean(assistantText+clean(value.delta));
      var locked=phrases.filter(function(p){return p.startMs<=clock&&playing&&clock>0;}),last=locked[locked.length-1],wordEnd=last?last.startWord+last.wordCount:0,tail=assistantText.trim().split(/\s+/).slice(wordEnd).join(' '),base=last?last.startMs+last.durationMs:0;
      var next=plan(tail,{context:context}).map(function(p,i){return Object.assign({},p,{index:locked.length+i,startMs:base+p.startMs,startWord:wordEnd+p.startWord});});phrases=locked.concat(next).slice(0,24);return true;
    }
    function playback(value){
      if(!active||paused||reduced||destroyed||!value||value.currentTurn===false||value.inputItemId&&turnId&&value.inputItemId!==turnId)return false;var id=clean(value.responseId||'').slice(0,200);
      if(id&&closedResponses.has(id))return false;
      if(id&&responseId&&id!==responseId){if(playing)return false;assistantText='';phrases=[];clock=0;index=-1;itemId='';}if(id)responseId=id;
      if(value.cleared){cancel();return true;}
      var starting=!playing&&value.playing===true;playing=value.playing===true;if(starting){mediaAt=null;mediaPositive=mediaDriven=false;if(Number.isInteger(value.startOffsetMs)&&value.startOffsetMs>=0&&value.startOffsetMs<=60000)clock=value.startOffsetMs;}lastTick=now();if(playing){listening=false;listenCueAt=null;}else{if(responseId){retire(responseId);}output=0;mediaAt=null;mediaPositive=mediaDriven=false;emit(null,0);}return true;
    }
    function level(value){if(!active||paused||reduced||destroyed)return;value=value||{};if(value.currentTurn===false||value.inputItemId&&turnId&&value.inputItemId!==turnId||value.responseId&&(closedResponses.has(value.responseId)||responseId&&value.responseId!==responseId))return false;var time=now();output=clamp(value.output,0,1);input=clamp(value.input,0,1);
      // A local media clock avoids stretching speech when rendering callbacks
      // are sparse. It is still an estimate: no provider word timestamps exist.
      // Only adjacent positive output samples admit a bounded media interval.
      var media=typeof value.outputTimeMs==='number'&&Number.isFinite(value.outputTimeMs)&&value.outputTimeMs>=0&&value.outputTimeMs<=86400000?value.outputTimeMs:null;
      if(playing&&media!==null){var delta=mediaAt===null?0:media-mediaAt;if(mediaAt!==null&&mediaPositive&&output>.015&&delta>=0&&delta<=2000)clock+=delta;mediaAt=media;mediaPositive=output>.015;mediaDriven=true;}else{mediaAt=null;mediaPositive=false;if(playing&&Object.prototype.hasOwnProperty.call(value,'outputTimeMs'))mediaDriven=true;}
      if(playing&&output>.015)lastOutputAt=time;if(listening&&input>.025){lastInputAt=time;if(inputRunAt===null)inputRunAt=time;inputObservedMs+=clamp(time-lastLevelAt,0,100);}lastLevelAt=time;tick();}
    function setListening(value){if(!active||paused||reduced||destroyed||listening===(value===true))return;listening=value===true;if(listening){playing=false;output=0;lastTick=now();emit(context==='support'||context==='repair'?'support':'attentive',context==='ordinary'?.28:.25);}else if(!playing)emit(null,0);}
    function tick(){
      var time=now(),dt=clamp(time-lastTick,0,100);lastTick=time;if(!active||paused||reduced||destroyed)return snapshot();
      if(playing){
        if(!mediaDriven&&time-lastOutputAt<220&&phrases.length)clock+=dt;
        var cue=phrases.find(function(p){return clock>=p.startMs&&clock<p.startMs+p.durationMs;});
        if(!cue){emit(null,0);index=-1;return snapshot();}
        if(index!==cue.index){index=cue.index;transitions++;try{onEvent({type:'phrase',index:index,kind:cue.kind,timing:'estimated-audio-activity'});}catch(e){}}
        var shape=envelope(clock-cue.startMs,cue),audible=time-lastOutputAt<220,accent=audible?Math.min(.1,output*.12):0;
        emit(cue.kind,audible?cue.intensity*(.18+.82*shape)+accent:cue.intensity*.12);
      }else if(listening){
        if(inputRunAt!==null&&time-lastInputAt>380){if(inputObservedMs>=800&&time-lastBackchannel>3200){listenCueAt=time;lastBackchannel=time;listenKind=context==='ordinary'?'attentive':'support';}inputRunAt=null;inputObservedMs=0;}
        var age=listenCueAt===null?Infinity:time-listenCueAt;var pulse=envelope(age,{durationMs:1100});
        emit(age<1100?listenKind:context==='ordinary'?'attentive':'support',(context==='ordinary'?.26:.22)+pulse*.18);
      }return snapshot();
    }
    function cancel(){if(responseId)retire(responseId);active=false;clear();responseId=itemId='';}
    function setPaused(value){paused=value===true;if(paused)cancel();}
    function setReducedMotion(value){reduced=value===true;if(reduced)cancel();}
    function snapshot(){return {active:active,playing:playing,listening:listening,paused:paused,reducedMotion:reduced,turnId:turnId,responseId:responseId,itemId:itemId,context:context,cue:current?Object.assign({},current):null,currentPhrase:index,phraseCount:phrases.length,clockMs:Math.round(clock),transitions:transitions,timing:'estimated-audio-activity',diagnosis:false};}
    return {beginTurn:beginTurn,transcript:transcript,playback:playback,level:level,setListening:setListening,tick:tick,cancel:cancel,setPaused:setPaused,setReducedMotion:setReducedMotion,snapshot:snapshot,destroy:function(){cancel();destroyed=true;}};
  }
  var api={KINDS:KINDS,plan:plan,envelope:envelope,contextFor:contextFor,create:create};if(typeof module==='object'&&module.exports)module.exports=api;scope.BritesConciergeExpression=api;
})(typeof window!=='undefined'?window:globalThis);
