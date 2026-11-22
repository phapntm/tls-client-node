// Real filesystem and child-process checks for managed runtime slot ownership.
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { once } from "node:events";
const require=createRequire(import.meta.url);
const fs=require("node:fs/promises");
const {claimRuntimeLock}=require("../dist/runtime-lock.js");
const script=fileURLToPath(import.meta.url);

function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};}
function childProcess(args){
  return spawn(process.execPath,[script,...args],{stdio:["ignore","ignore","pipe","ipc"],windowsHide:true});
}
if(process.argv[2]==="--abandon"){
  const lock=await claimRuntimeLock(process.argv[3]);
  assert.ok(lock);
  process.send({type:"ready"});
  process.disconnect();
}else if(process.argv[2]==="--worker"){
  let held=[];
  process.on("message",async message=>{
    try{
      if(message.type==="claim"){
        const results=await Promise.all(Array.from({length:8},async()=>{
          for(let index=0;index<64;index++){
            const lock=await claimRuntimeLock(path.join(message.root,"slot-"+index));
            if(lock){held.push(lock);return index;}
          }
          throw Error("No runtime slot available");
        }));
        process.send({type:"claimed",slots:results});
      }else if(message.type==="release"){
        await Promise.all(held.map(lock=>lock.release()));held=[];
        process.send({type:"released"});
      }else if(message.type==="finish"){process.disconnect();}
    }catch(error){process.send({type:"failed",message:error.stack??error.message});}
  });
  process.send({type:"ready"});
}else{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),"tls-client-lock-smoke-"));
  const children=[];
  const scenariosChecked=[];
  const watchdog=setTimeout(()=>{for(const child of children)child.kill();console.error("Runtime lock smoke test timed out");process.exit(1);},45000);
  async function scenario(name,run){
    const directory=path.join(root,name);await fs.mkdir(directory);
    await run(directory);scenariosChecked.push(name);
  }
  async function abandoned(lockPath){
    const child=childProcess(["--abandon",lockPath]);children.push(child);
    child.stderr.on("data",data=>process.stderr.write(data));
    const exited=once(child,"exit");await once(child,"message");
    assert.deepEqual(await exited,[0,null]);
  }
  const originalWrite=fs.writeFile,originalUnlink=fs.unlink,originalRead=fs.readFile,originalKill=process.kill;
  try{
    await scenario("one-owner-and-idempotent-release",async directory=>{
      const target=path.join(directory,".lock");
      const results=await Promise.all(Array.from({length:32},()=>claimRuntimeLock(target)));
      const winners=results.filter(Boolean);assert.equal(winners.length,1);
      await winners[0].release();
      const next=await claimRuntimeLock(target);assert.ok(next);
      await winners[0].release();
      assert.equal(await claimRuntimeLock(target),undefined);
      await next.release();
      assert.deepEqual(await fs.readdir(directory),[]);
    });
    await scenario("owner-published-together-with-lock",async directory=>{
      const target=path.join(directory,".lock"),preparing=deferred(),resume=deferred();
      let held=false,first,second;
      fs.writeFile=async(file,...args)=>{
        if(!held&&path.dirname(file).startsWith(target+".")){held=true;preparing.resolve();await resume.promise;}
        return originalWrite(file,...args);
      };
      try{
        first=claimRuntimeLock(target);first.catch(()=>{});
        await preparing.promise;
        second=await claimRuntimeLock(target);assert.ok(second);
        resume.resolve();assert.equal(await first,undefined);
        assert.equal(await claimRuntimeLock(target),undefined);
      }finally{
        resume.resolve();fs.writeFile=originalWrite;await first?.catch(()=>{});
        await second?.release();
      }
      assert.deepEqual(await fs.readdir(directory),[]);
    });
    await scenario("dead-owner-reclaimed-once",async directory=>{
      const target=path.join(directory,".lock");await abandoned(target);
      const results=await Promise.all(Array.from({length:32},()=>claimRuntimeLock(target)));
      const winners=results.filter(Boolean);assert.equal(winners.length,1);await winners[0].release();
      assert.deepEqual(await fs.readdir(directory),[]);
    });
    await scenario("stale-reaper-cannot-delete-new-owner",async directory=>{
      const target=path.join(directory,".lock");await abandoned(target);
      const [oldOwner]=await fs.readdir(target),oldPath=path.join(target,oldOwner);
      const firstReaper=deferred(),bothReapers=deferred(),resumeSecond=deferred();
      let removals=0,first,second,firstLock,secondLock;
      fs.unlink=async(file,...args)=>{
        if(file===oldPath){
          removals++;
          if(removals===1){firstReaper.resolve();await bothReapers.promise;}
          else if(removals===2){bothReapers.resolve();await resumeSecond.promise;}
        }
        return originalUnlink(file,...args);
      };
      try{
        first=claimRuntimeLock(target);first.catch(()=>{});
        await firstReaper.promise;
        second=claimRuntimeLock(target);second.catch(()=>{});
        firstLock=await first;assert.ok(firstLock);
        resumeSecond.resolve();secondLock=await second;assert.equal(secondLock,undefined);
        assert.equal(await claimRuntimeLock(target),undefined);
      }finally{
        resumeSecond.resolve();bothReapers.resolve();fs.unlink=originalUnlink;
        await Promise.allSettled([first,second].filter(Boolean));
        await firstLock?.release();await secondLock?.release();
      }
      assert.deepEqual(await fs.readdir(directory),[]);
    });
    await scenario("former-owner-release-keeps-replacement",async directory=>{
      const target=path.join(directory,".lock"),old=await claimRuntimeLock(target);
      const [owner]=await fs.readdir(target);
      await fs.unlink(path.join(target,owner));await fs.rmdir(target);
      const current=await claimRuntimeLock(target);assert.ok(current);
      await old.release();assert.equal(await claimRuntimeLock(target),undefined);
      await current.release();
    });
    await scenario("legacy-pid-locks-and-empty-directories",async directory=>{
      const target=path.join(directory,".lock");
      for(const value of ["", "invalid", String(process.pid)]){
        await fs.writeFile(target,value);assert.equal(await claimRuntimeLock(target),undefined);
        assert.equal(await fs.readFile(target,"utf8"),value);await fs.unlink(target);
      }
      await fs.writeFile(target,"1073741823");
      const results=await Promise.all(Array.from({length:16},()=>claimRuntimeLock(target)));
      const winners=results.filter(Boolean);assert.equal(winners.length,1);await winners[0].release();
      await fs.mkdir(target);
      const lock=await claimRuntimeLock(target);assert.ok(lock);await lock.release();
      await fs.mkdir(target);await fs.writeFile(path.join(target,"unknown-owner"),"");
      assert.equal(await claimRuntimeLock(target),undefined);
      assert.deepEqual(await fs.readdir(target),["unknown-owner"]);
    });
    await scenario("permission-errors-do-not-prove-owner-is-dead",async directory=>{
      const target=path.join(directory,".lock");
      const lock=await claimRuntimeLock(target);assert.ok(lock);
      for(const code of ["EPERM","EACCES"]){
        process.kill=()=>{throw Object.assign(Error("fixture permission error"),{code});};
        assert.equal(await claimRuntimeLock(target),undefined);
      }
      process.kill=originalKill;await lock.release();
      await fs.writeFile(target,String(process.pid));
      fs.readFile=async(file,...args)=>{if(file===target)throw Object.assign(Error("fixture"),{code:"EACCES"});return originalRead(file,...args);};
      assert.equal(await claimRuntimeLock(target),undefined);
      fs.readFile=originalRead;assert.equal(await fs.readFile(target,"utf8"),String(process.pid));
    });
    await scenario("eight-processes-64-slots-eight-waves",async directory=>{
      const workers=Array.from({length:8},()=>{const child=childProcess(["--worker"]);children.push(child);child.stderr.on("data",data=>process.stderr.write(data));return child;});
      await Promise.all(workers.map(child=>once(child,"message")));
      const exchange=(child,type,message)=>new Promise((resolve,reject)=>{
        const listener=response=>{
          child.off("message",listener);
          if(response.type==="failed")reject(Error(response.message));
          else{assert.equal(response.type,type);resolve(response);}
        };
        child.once("message",listener);child.send(message);
      });
      for(let wave=0;wave<8;wave++){
        const waveRoot=path.join(directory,String(wave));await fs.mkdir(waveRoot);
        const results=await Promise.all(workers.map(child=>exchange(child,"claimed",{type:"claim",root:waveRoot})));
        const slots=results.flatMap(result=>result.slots);
        assert.equal(slots.length,64);assert.equal(new Set(slots).size,64);
        await Promise.all(workers.map(child=>exchange(child,"released",{type:"release"})));
        assert.deepEqual(await fs.readdir(waveRoot),[]);
      }
      const exits=workers.map(child=>once(child,"exit"));
      for(const child of workers)child.send({type:"finish"});
      const statuses=await Promise.all(exits);for(const status of statuses)assert.deepEqual(status,[0,null]);
    });
    console.log(JSON.stringify({ok:true,scenariosChecked,crossProcessAllocations:512}));
  }finally{
    fs.writeFile=originalWrite;fs.unlink=originalUnlink;fs.readFile=originalRead;process.kill=originalKill;
    for(const child of children)if(child.exitCode===null&&child.signalCode===null)child.kill();
    clearTimeout(watchdog);
    assert.equal(path.dirname(path.resolve(root)),path.resolve(os.tmpdir()));
    assert.ok(path.basename(root).startsWith("tls-client-lock-smoke-"));
    await fs.rm(root,{recursive:true,force:true});
  }
}
