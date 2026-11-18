const http=require('http'),{WebSocketServer}=require('ws');
const srv=http.createServer((q,r)=>r.end('Servidor de Ciudad Telarana activo'));
const wss=new WebSocketServer({server:srv}),rooms=new Map();
wss.on('connection',ws=>{let room=null,id=null;
 ws.on('message',raw=>{if(raw.length>4096)return;let m;try{m=JSON.parse(raw)}catch{return}
  if(m.t==='join'&&!room){id=String(m.id||'').slice(0,12)||Math.random().toString(36).slice(2,9);const c=String(m.room||'x').slice(0,24);
   room=rooms.get(c)||{c,p:new Map(),dirty:false};if(room.p.size>=30){ws.close();return}rooms.set(c,room);room.p.set(id,{ws,p:{}});room.dirty=true}
  else if(m.t==='pres'&&room){const e=room.p.get(id);if(e&&m.p&&typeof m.p==='object'){e.p=m.p;room.dirty=true}}
  else if(m.t==='emit'&&room){const s=JSON.stringify({t:'emit',topic:String(m.topic).slice(0,30),data:m.data,from:id});for(const e of room.p.values())if(e.ws.readyState===1)e.ws.send(s)}});
 ws.on('close',()=>{if(room){room.p.delete(id);room.dirty=true;if(!room.p.size)rooms.delete(room.c)}})});
setInterval(()=>{for(const r of rooms.values()){if(!r.dirty)continue;r.dirty=false;const s=JSON.stringify({t:'peers',peers:[...r.p].map(([id,e])=>({id,p:e.p}))});for(const e of r.p.values())if(e.ws.readyState===1)e.ws.send(s)}},60);
srv.listen(process.env.PORT||8080);
