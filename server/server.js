const http=require('http');
const {WebSocketServer}=require('ws');
const server=http.createServer((q,r)=>r.end('ok'));
const wss=new WebSocketServer({server});
const users=new Map();
function send(s,o){if(s.readyState===1)s.send(JSON.stringify(o))}
function list(){const n=[...users.keys()];for(const s of users.values())send(s,{t:'users',users:n})}
wss.on('connection',ws=>{
  ws.alive=true;
  ws.on('pong',()=>{ws.alive=true});
  ws.on('message',(d,isBin)=>{
    if(isBin){
      if(!ws.name||d.length<2)return;
      const l=d[0];
      const to=users.get(d.subarray(1,1+l).toString());
      if(!to||to.readyState!==1)return;
      const me=Buffer.from(ws.name);
      to.send(Buffer.concat([Buffer.from([me.length]),me,d.subarray(1+l)]),{binary:true});
      return;
    }
    let m;try{m=JSON.parse(d.toString())}catch{return}
    if(m.t==='login'){
      const n=String(m.name||'').trim().slice(0,20);
      if(!n)return;
      const old=users.get(n);
      if(old&&old!==ws){old.name=null;old.terminate()}
      ws.name=n;users.set(n,ws);
      send(ws,{t:'ok'});list();return;
    }
    if(!ws.name)return;
    const to=users.get(m.to);
    if(to){m.from=ws.name;send(to,m)}
  });
  ws.on('close',()=>{
    if(ws.name&&users.get(ws.name)===ws){users.delete(ws.name);list()}
  });
  ws.on('error',()=>{});
});
setInterval(()=>{
  wss.clients.forEach(c=>{
    if(!c.alive)return c.terminate();
    c.alive=false;c.ping();
  });
},30000);
server.listen(process.env.PORT||3000);
