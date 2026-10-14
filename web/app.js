const API = "http://localhost:8080";
const metricNames = [["submitted","Submitted"],["queued","Queued"],["running","Running"],["succeeded","Succeeded"],["failed","Failed"],["retried","Retries"],["avg_latency_ms","Avg latency (ms)"],["p95_latency_ms","p95 latency (ms)"]];
async function get(path){const r=await fetch(API+path);if(!r.ok)throw new Error("HTTP "+r.status);return r.json();}
function esc(s){return String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
async function refresh(){try{
 const [s,j]=await Promise.all([get("/stats"),get("/jobs")]);document.querySelector("#health").textContent="API online";
 document.querySelector("#metrics").innerHTML=metricNames.map(([k,label])=>`<article class="metric"><span>${label}</span><strong>${Number(s[k]??0).toLocaleString(undefined,{maximumFractionDigits:2})}</strong></article>`).join("");
 document.querySelector("#jobs").innerHTML=j.map(x=>`<tr><td>${x.id}</td><td>${esc(x.name)}</td><td>${x.priority}</td><td><span class="status ${esc(x.status)}">${esc(x.status)}</span></td><td>${esc(x.updated_at)}</td><td>${esc(x.error)}</td></tr>`).join("")||'<tr><td colspan="6">No jobs submitted yet.</td></tr>';
 }catch(e){document.querySelector("#health").textContent="API offline";document.querySelector("#message").textContent="Start the C++ server, then refresh. "+e.message;}}
document.querySelector("#jobForm").addEventListener("submit",async e=>{e.preventDefault();const payload={name:document.querySelector("#name").value,priority:+document.querySelector("#priority").value,failures:+document.querySelector("#failures").value};try{const r=await fetch(API+"/jobs",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});const data=await r.json();if(!r.ok)throw new Error(data.error||"Request failed");document.querySelector("#message").textContent="Queued job #"+data.id;refresh();}catch(err){document.querySelector("#message").textContent="Could not submit: "+err.message;}});
document.querySelector("#refresh").addEventListener("click",refresh);refresh();setInterval(refresh,2500);
