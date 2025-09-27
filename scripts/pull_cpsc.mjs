// Node 18+; no deps
import { promises as fs } from "fs";
import path from "path";

const MANIFEST_PATH = "config/topic_manifest.json";
const DATA_DIR = "docs/data";
const RETAIN_DAYS = parseInt(process.env.RETAIN_DAYS || "14", 10);

const BASE = "https://www.saferproducts.gov/RestWebServices/Recall";

function norm(s=""){return s.toLowerCase().replace(/\s+/g," ").trim();}
function withinDays(iso, days){
  const dt = iso ? new Date(iso) : null; if(!dt) return false;
  return (Date.now() - dt.getTime())/86400000 <= days;
}
async function fetchJSON(u){
  const r = await fetch(u, { headers:{accept:"application/json"}});
  if(!r.ok){ console.error("CPSC fetch failed", r.status, await r.text()); return []; }
  return r.json();
}
function qs(params){ return Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&"); }

async function main(){
  const manifest = JSON.parse(await fs.readFile(MANIFEST_PATH,"utf-8"));
  await fs.mkdir(DATA_DIR, { recursive:true });

  // read or init index
  const indexPath = path.join(DATA_DIR,"index.json");
  let index = { latest:{}, files:[] };
  try{ index = JSON.parse(await fs.readFile(indexPath,"utf-8")); }catch{}

  const recencyDays = manifest.recency_days ?? 30;

  for(const q of manifest.queries || []){
    const topicId = q.id;
    const include = q.include || [];
    const seen = new Set();
    const items = [];

    for(const term of include){
      // hit by RecallDescription and Title (CPSC shows these filters in examples)
      // JSON format
      const urls = [
        `${BASE}?${qs({ format:"json", RecallDescription:term })}`,
        `${BASE}?${qs({ format:"json", Title:term })}`
      ];
      for(const url of urls){
        const arr = await fetchJSON(url);
        for(const r of arr || []){
          const id = r.RecallID || r.RecallNumber || `${r.RecallDate}-${r.Title}`;
          if(!id || seen.has(id)) continue;

          const date = r.RecallDate || r.LastPublishDate || r.PostedDate || null;
          if(date && !withinDays(date, recencyDays)) continue;

          // Compose one clean text field; drop PII/contacts
          const title = (r.Title||"").trim();
          const desc = (r.Description||r.RecallDescription||"").replace(/\s+/g," ").trim();
          const text = [title, desc].filter(Boolean).join(" — ");
          if(text.length < 30) continue;

          items.push({
            id,
            text,
            timestamp: date || null,
            lang: "en",
            hazard: r.Hazard || null,
            remedy: r.Remedy || null,
            product: (r.Products && r.Products[0]?.Name) || r.ProductName || null
          });
          seen.add(id);
        }
        await new Promise(r=>setTimeout(r,150));
      }
    }

    const snapshot = { source:"cpsc", topic_id:topicId, fetched_at:new Date().toISOString(), items };
    const fname = `cpsc_${topicId}_${dateStamp()}.json`;
    await fs.writeFile(path.join(DATA_DIR,fname), JSON.stringify(snapshot,null,2));
    index.latest[topicId] = fname;
    if(!index.files.includes(fname)) index.files.push(fname);
    console.log(`CPSC wrote ${fname} with ${items.length} items`);
  }

  await pruneOldSnapshots(DATA_DIR, RETAIN_DAYS, index);
  await fs.writeFile(indexPath, JSON.stringify(index,null,2));
}

async function pruneOldSnapshots(dir, retainDays, index){
  const files = await fs.readdir(dir);
  const keep = new Set();
  const now = new Date();
  for(const f of files){
    const m = f.match(/^(cpsc_.+?)_(\d{4}-\d{2}-\d{2})\.json$/);
    if(!m){ keep.add(f); continue; }
    const d = new Date(m[2]+"T00:00:00Z");
    const age = (now - d)/86400000;
    if(age <= retainDays) keep.add(f);
    else { await fs.rm(path.join(dir,f)).catch(()=>{}); console.log("Pruned old:", f); }
  }
  index.files = index.files.filter(f=>keep.has(f));
}

function dateStamp(d=new Date()){
  const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,"0"), da=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${da}`;
}

main().catch(e=>{ console.error(e); process.exit(1); });
