// Managed lifecycle checks use real HTTP/WebSocket fixtures and mock only the
// spawned executable. No binary is downloaded and no real API process is needed.
import assert from "node:assert/strict";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { EventEmitter, once } from "node:events";
import { PassThrough } from "node:stream";
import { createRequire } from "node:module";
import { setTimeout as sleep } from "node:timers/promises";
const require=createRequire(import.meta.url);
const fs=require("node:fs/promises"),processModule=require("node:child_process");
const binary=require("../dist/binary.js");
const {TLSClient,TLSWebSocket}=require("../dist/index.js");
const {WebSocketServer}=require("ws");
const originalSpawn=processModule.spawn,originalEnsure=binary.ensureBinary,originalWrite=binary.writeConfigFile,originalCopy=fs.copyFile;
const root=await fs.mkdtemp(path.join(os.tmpdir(),"tls-client-managed-smoke-"));
const scenariosChecked=[],unhandled=[];
const onUnhandled=error=>unhandled.push(error);
process.on("unhandledRejection",onUnhandled);
const watchdog=setTimeout(()=>{console.error("Managed runtime smoke test timed out");process.exit(1);},45000);
function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};}
async function fixture(name,behavior,run){
  const directory=path.join(root,name);await fs.mkdir(directory);
  const executable=path.join(directory,"fixture-api.exe");
  await fs.writeFile(executable,"managed-runtime-fixture");
  const children=[],clients=[],spawned=deferred();
  let ensureCalls=0,forwardCalls=0,earlyCalls=0;
  const state={directory,executable,children,spawned,forwarded:deferred(),healthReady:behavior.ready!==false,releaseListen:()=>behavior.listenGate?.resolve(),forwardCalls:()=>forwardCalls};
  state.client=(overrides={})=>{
    const client=new TLSClient({runtimeMode:"managed",runtimeDir:path.join(directory,"runtime"),runtimeSlots:64,startupTimeoutMs:2000,requestTimeoutMs:1500,...overrides});
    clients.push(client);return client;
  };
  binary.ensureBinary=async()=>{ensureCalls++;return{executablePath:executable,version:"fixture"};};
  processModule.spawn=(file,args,options)=>{
    if(behavior.throwSpawn)throw Object.assign(Error("fixture spawn failure"),{code:"ENOENT"});
    const child=new EventEmitter();
    Object.assign(child,{pid:500000+children.length,exitCode:null,signalCode:null,stdout:new PassThrough(),stderr:new PassThrough(),cwd:options.cwd,kills:0,probed:deferred(),closed:deferred()});
    children.push(child);spawned.resolve(child);
    child.kill=(signal="SIGTERM")=>{
      child.kills++;
      if(child.exitCode!==null||child.signalCode!==null)return false;
      child.signalCode=signal;
      for(const socket of child.wss?.clients??[])socket.terminate();
      child.server?.closeAllConnections();
      if(child.server?.listening)child.server.close(()=>child.closed.resolve());
      else child.closed.resolve();
      child.emit("exit",null,signal);
      child.stdout.end();child.stderr.end();return true;
    };
    if(behavior.errorSpawn){
      child.pid=undefined;
      queueMicrotask(()=>{child.emit("error",Object.assign(Error("fixture async spawn failure"),{code:"ENOENT"}));child.closed.resolve();});
      return child;
    }
    if(behavior.portConflictOnce&&children.length===1){
      queueMicrotask(()=>{child.stdout.write("listen tcp: address already in use");child.exitCode=1;child.emit("exit",1,null);child.closed.resolve();});
      return child;
    }
    (async()=>{
      const config=await fs.readFile(path.join(options.cwd,"config.dist.yml"),"utf8");
      const port=Number(/api:\r?\n  port: (\d+)/.exec(config)?.[1]);
      const key=/api_auth_keys: \["([^"]+)"\]/.exec(config)?.[1];
      assert.ok(port&&key);child.port=port;
      child.server=http.createServer(async(req,res)=>{
        if(req.headers["x-api-key"]!==key){res.writeHead(401).end();return;}
        const chunks=[];for await(const chunk of req)chunks.push(chunk);
        const payload=JSON.parse(Buffer.concat(chunks).toString()||"{}");
        if(payload.sessionId==="tls-client-healthcheck"){
          child.probed.resolve();
          if(behavior.hangHealth)return;
          res.writeHead(state.healthReady?200:503,{"content-type":"application/json"});
          res.end(JSON.stringify({id:"health",success:true}));return;
        }
        if(!state.healthReady)earlyCalls++;
        if(req.url==="/api/forward"){
          forwardCalls++;state.forwarded.resolve();
          if(behavior.holdForward)return;
          res.writeHead(200,{"content-type":"application/json"});
          res.end(JSON.stringify({id:"fixture",status:200,body:"ok",target:payload.requestUrl,sessionId:payload.sessionId,headers:{},cookies:{}}));
        }else{
          res.writeHead(200,{"content-type":"application/json"});
          res.end(JSON.stringify({id:payload.sessionId,success:true}));
        }
      });
      child.wss=new WebSocketServer({noServer:true});
      child.server.on("upgrade",(req,socket,head)=>{
        if(!state.healthReady){earlyCalls++;socket.destroy();return;}
        child.wss.handleUpgrade(req,socket,head,ws=>ws.on("message",(data,binary)=>ws.send(data,{binary})));
      });
      child.server.on("error",error=>child.emit("error",error));
      if(behavior.listenGate)await behavior.listenGate.promise;
      if(child.signalCode===null)child.server.listen(port,"127.0.0.1");
    })().catch(error=>child.emit("error",error));
    return child;
  };
  try{
    await run(state);
    assert.equal(earlyCalls,0,"An application request reached an unready runtime");
    scenariosChecked.push(name);
  }finally{
    state.healthReady=true;behavior.listenGate?.resolve();
    await Promise.allSettled(clients.map(client=>client.stop()));
    for(const child of children)if(child.pid&&child.exitCode===null&&child.signalCode===null)child.kill();
    await Promise.all(children.map(child=>child.closed.promise));
    for(const child of children)await new Promise(resolve=>child.wss?child.wss.close(resolve):resolve());
    processModule.spawn=originalSpawn;binary.ensureBinary=originalEnsure;binary.writeConfigFile=originalWrite;fs.copyFile=originalCopy;
  }
  return{ensureCalls,forwardCalls,earlyCalls};
}
try{
  await fixture("late-requests-and-websocket-wait-for-health",{ready:false},async state=>{
    const client=state.client(),sessions=Array.from({length:16},()=>client.session());
    const first=sessions[0].get("https://example.test/first");first.catch(()=>{});
    const child=await state.spawned.promise;await child.probed.promise;
    let startFinished=false,requestFinished=false,closeFinished=false;
    const starting=client.start().then(()=>{startFinished=true;});
    const request=sessions[1].get("https://example.test/late").then(response=>{requestFinished=true;return response;});
    request.catch(()=>{});
    const closing=client.session().close().then(()=>{closeFinished=true;});closing.catch(()=>{});
    const ws=sessions[2].websocket("wss://example.test/socket");
    ws.on("error",()=>{});
    const opened=once(ws,"open");opened.catch(()=>{});
    assert.equal(ws.readyState,TLSWebSocket.CONNECTING);
    await sleep(30);
    assert.equal(startFinished,false);assert.equal(requestFinished,false);assert.equal(closeFinished,false);
    assert.equal(client.runtime,undefined);assert.equal(state.children.length,1);
    state.healthReady=true;
    const [a,b]=await Promise.all([first,request,starting,closing,opened]);
    assert.equal(a.status,200);assert.equal(b.status,200);assert.equal(state.children.length,1);
    const closed=once(ws,"close");ws.close();await closed;
    await Promise.all(sessions.map(session=>session.close()));
  });
  await fixture("first-requests-wait-for-delayed-listen",{listenGate:deferred()},async state=>{
    const client=state.client(),session=client.session();
    const pending=Array.from({length:16},(_,i)=>session.get("https://example.test/"+i));
    for(const promise of pending)promise.catch(()=>{});
    await state.spawned.promise;await sleep(30);
    assert.equal(client.runtime,undefined);
    // The mock executable has not opened its API port yet.
    // Release through the scenario's gate only after all requests are waiting.
    state.healthReady=true;
    state.releaseListen();
    const responses=await Promise.all(pending);assert.ok(responses.every(response=>response.status===200));
    await session.close();
  });
  await fixture("32-clients-own-32-distinct-slots",{},async state=>{
    const clients=Array.from({length:32},()=>state.client({runtimeSlots:32}));
    const sessions=clients.map(client=>client.session());
    const responses=await Promise.all(sessions.map(session=>session.get("https://example.test/")));
    assert.ok(responses.every(response=>response.status===200));
    assert.equal(state.children.length,32);assert.equal(new Set(state.children.map(child=>child.cwd)).size,32);
    await Promise.all(sessions.map(session=>session.close()));
  });
  for(const type of ["throwSpawn","errorSpawn"]){
    const behavior={[type]:true};
    await fixture(type+"-cleans-up-and-retries",behavior,async state=>{
      const client=state.client({runtimeSlots:1});
      await assert.rejects(client.start(),error=>error.code==="ERR_STARTUP"&&error.cause?.code==="ENOENT"||error.code==="ERR_STARTUP"&&error.cause?.cause?.code==="ENOENT");
      assert.deepEqual((await fs.readdir(path.join(state.directory,"runtime/fixture/slot-0"))).filter(name=>name.includes(".lock")),[]);
      behavior[type]=false;
      const response=await client.session().get("https://example.test/retry");assert.equal(response.status,200);
    });
  }
  await fixture("config-write-failure-releases-slot",{},async state=>{
    binary.writeConfigFile=async(...args)=>{await originalWrite(...args);throw Object.assign(Error("fixture config failure"),{code:"EACCES"});};
    const client=state.client({runtimeSlots:1});await assert.rejects(client.start(),{code:"ERR_STARTUP"});
    assert.equal(state.children.length,0);
    await assert.rejects(fs.access(path.join(state.directory,"runtime/fixture/slot-0/config.dist.yml")),{code:"ENOENT"});
    binary.writeConfigFile=originalWrite;await client.start();assert.equal(state.children.length,1);
    assert.equal(path.basename(client.runtime.runtimeDir),"slot-0");
  });
  await fixture("partial-copy-is-repaired-and-failed-copy-is-not-published",{},async state=>{
    const slot=path.join(state.directory,"runtime/fixture/slot-0");await fs.mkdir(slot,{recursive:true});
    const copied=path.join(slot,path.basename(state.executable));await fs.writeFile(copied,"x");
    let fail=true;
    fs.copyFile=async(source,destination,...args)=>{
      if(fail){await fs.writeFile(destination,"partial");throw Object.assign(Error("fixture copy failure"),{code:"EIO"});}
      return originalCopy(source,destination,...args);
    };
    const client=state.client({runtimeSlots:1});
    await assert.rejects(client.start(),{code:"EIO"});
    assert.equal(await fs.readFile(copied,"utf8"),"x");
    assert.equal((await fs.readdir(slot)).some(name=>name.endsWith(".tmp")||name===".lock"),false);
    fail=false;await client.start();
    assert.equal(await fs.readFile(copied,"utf8"),await fs.readFile(state.executable,"utf8"));
  });
  await fixture("startup-port-conflict-retries-without-leaking-lock",{portConflictOnce:true},async state=>{
    const client=state.client({runtimeSlots:1});await client.start();
    assert.equal(state.children.length,2);assert.equal(state.children[0].cwd,state.children[1].cwd);
    assert.equal((await client.session().get("https://example.test/")).status,200);
  });
  await fixture("health-probe-respects-startup-timeout",{hangHealth:true},async state=>{
    const client=state.client({runtimeSlots:1,startupTimeoutMs:100,requestTimeoutMs:10000}),started=Date.now();
    await assert.rejects(client.start(),error=>error.code==="ERR_STARTUP"&&error.cause?.code==="ERR_STARTUP_TIMEOUT");
    assert.ok(Date.now()-started<2000,"Health probe exceeded the startup deadline");
    assert.equal(state.children[0].kills,1);
  });
  await fixture("stop-during-startup-and-concurrent-stops",{ready:false},async state=>{
    const client=state.client({runtimeSlots:1}),starting=client.start();
    const child=await state.spawned.promise;await child.probed.promise;
    let stopped=false;
    const stopA=client.stop().then(()=>{stopped=true;}),stopB=client.stop();
    await sleep(20);assert.equal(stopped,false);
    state.healthReady=true;await Promise.all([starting,stopA,stopB]);
    assert.equal(client.runtime,undefined);assert.equal(child.kills,1);
    await client.start();assert.equal(state.children.length,2);
  });
  await fixture("exited-runtime-is-replaced-on-next-operation",{},async state=>{
    const client=state.client({runtimeSlots:1}),session=client.session();
    assert.equal((await session.get("https://example.test/")).status,200);
    state.children[0].kill();
    const responses=await Promise.all(Array.from({length:8},()=>session.get("https://example.test/restarted")));
    assert.ok(responses.every(response=>response.status===200));assert.equal(state.children.length,2);
    assert.equal(state.children[0].cwd,state.children[1].cwd);
    await session.close();
  });
  const interrupted={holdForward:true};
  await fixture("inflight-request-is-not-replayed-after-exit",interrupted,async state=>{
    const client=state.client({runtimeSlots:1}),session=client.session();
    const pending=session.post("https://example.test/write",{body:"one"});pending.catch(()=>{});
    await state.forwarded.promise;state.children[0].kill();
    await assert.rejects(pending,{code:"ERR_RUNTIME_EXITED"});
    assert.equal(state.children.length,1);
    assert.equal(state.forwardCalls(),1);
    interrupted.holdForward=false;
    await session.close();
    assert.equal(state.children.length,2);assert.equal(state.forwardCalls(),1);
    const recovered=client.session();
    assert.equal((await recovered.get("https://example.test/recover")).status,200);
    assert.equal(state.children.length,2);await recovered.close();
  });
  await sleep(0);assert.deepEqual(unhandled,[]);
  console.log(JSON.stringify({ok:true,scenariosChecked}));
}finally{
  processModule.spawn=originalSpawn;binary.ensureBinary=originalEnsure;binary.writeConfigFile=originalWrite;fs.copyFile=originalCopy;
  process.off("unhandledRejection",onUnhandled);clearTimeout(watchdog);
  assert.equal(path.dirname(path.resolve(root)),path.resolve(os.tmpdir()));
  assert.ok(path.basename(root).startsWith("tls-client-managed-smoke-"));
  await fs.rm(root,{recursive:true,force:true});
}
