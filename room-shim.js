// Login-free "room" for the standalone (GitHub Pages) copy.
// Mimics the small part of the artifact room API the page uses (presence / onPeers / peers)
// over a public MQTT broker via WebSocket. Nothing is stored; peers expire when silent.
(function(){
  const BROKERS=['wss://broker.emqx.io:8084/mqtt','wss://broker.hivemq.com:8884/mqtt','wss://test.mosquitto.org:8081'];
  const BASE='gp4f-busan-workation-7c2e/v1/p/';
  const me=Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-4);
  const peers=new Map(); // id -> {presence,t}
  const subs=[];let mine=null,client=null,lastPub=0,pending=null,ready=null;
  const list=()=>{const now=Date.now(),out=[];
    if(mine)out.push({peer:me,kind:'viewer',presence:mine,isMe:true,sameTab:true,guest:false});
    for(const [id,v] of peers){if(now-v.t>12000){peers.delete(id);continue;}out.push({peer:id,kind:'viewer',presence:v.presence,isMe:false,sameTab:false,guest:false});}
    return out;};
  const emit=()=>{const ps=list();subs.forEach(f=>{try{f({peers:ps});}catch(e){}});};
  function pub(){if(!client||!client.connected||!mine)return;lastPub=Date.now();pending=null;
    try{client.publish(BASE+me,JSON.stringify(mine),{qos:0});}catch(e){}}
  function connect(i){return new Promise(res=>{
    if(!window.mqtt||i>=BROKERS.length)return res(false);
    let done=false;const c=mqtt.connect(BROKERS[i],{clientId:'gp4f_'+me+'_'+i,clean:true,connectTimeout:6000,reconnectPeriod:3000,keepalive:20,
      will:{topic:BASE+me,payload:'',qos:0,retain:false}});
    const fail=()=>{if(done)return;done=true;try{c.end(true);}catch(e){}connect(i+1).then(res);};
    const to=setTimeout(fail,7000);
    c.on('connect',()=>{if(done)return;done=true;clearTimeout(to);client=c;c.subscribe(BASE+'+');pub();res(true);});
    c.on('error',()=>{if(!client)fail();});
    c.on('message',(topic,buf)=>{const id=topic.slice(BASE.length);if(id===me)return;const s=buf.toString();
      if(!s){peers.delete(id);emit();return;}
      try{const p=JSON.parse(s);if(p&&typeof p==='object'&&s.length<800){const isNew=!peers.has(id);peers.set(id,{presence:p,t:Date.now()});if(isNew)emit();}}catch(e){}});
  });}
  const room={
    presence(p){mine=p;const wait=150-(Date.now()-lastPub);
      if(wait<=0)pub();else if(!pending)pending=setTimeout(pub,wait);
      return Promise.resolve();},
    onPeers(fn,onErr){subs.push(fn);fn({peers:list()});return ()=>{const k=subs.indexOf(fn);if(k>=0)subs.splice(k,1);};},
    peers:list
  };
  // heartbeat keeps us visible; regular refresh lets movement show up smoothly
  setInterval(pub,3000);
  setInterval(emit,200);
  addEventListener('pagehide',()=>{try{client&&client.publish(BASE+me,'',{qos:0});}catch(e){}});
  window.claude={use:async(name)=>{if(name!=='room')return null;
    if(!ready)ready=connect(0);const ok=await ready;return ok?room:null;}};
})();
