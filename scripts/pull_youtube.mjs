// Node 18+ (built-in fetch)
import { promises as fs } from "fs";
import path from "path";

const API_KEY = process.env.YT_API_KEY;
if (!API_KEY) { console.error("Missing env YT_API_KEY"); process.exit(1); }

// Tunables
const MANIFEST_PATH = "config/topic_manifest.json";
const DATA_DIR = "docs/data";
const RETAIN_DAYS = parseInt(process.env.RETAIN_DAYS || "14", 10);  // keep only last N days
const REGION = process.env.YT_REGION || "US";
const LANG = process.env.YT_LANG || "en";

// Relevance filters (simple, effective, free)
const NEGATIVE_HINTS = [
  "leak","leaks","drip","spill","spills","smell","odor","stinks",
  "hard","difficult","impossible","pain","painful","noisy","loud",
  "heavy","bulky","broken","broke","breaks","fragile","flimsy","cheap",
  "slow","late","refund","return","bad","worst","rust","mold","crack","cracked","stain","stains","dirty"
];
const PRODUCT_WORDS = [
  "bought","buy","using","works","doesn't","doesnt","stopped","after","week","month",
  "review","problem","issue","quality","clean","size","fit","seal","cap","lid","battery","charge"
];
const IRRELEVANT_HINTS = [
  "subscribe","channel","like the video","handsome","cute","song","music","lyrics","asmr","shorts"
];

function normalizeText(s=""){
  return s.toLowerCase()
    .replace(/https?:\/\/\S+/g," ")
    .replace(/[^\p{L}\p{N}\s]/gu," ")
    .replace(/\s+/g," ")
    .trim();
}
function isLikelyEnglish(s){
  const letters = (s.match(/[a-z]/g)||[]).length;
  const nonlat  = (s.match(/[^\x00-\x7F]/g)||[]).length;
  return letters >= 15 && nonlat/Math.max(1,(s.length)) < 0.15;
}
function isRelevantComment(raw){
  const t = normalizeText(raw);
  if (t.length < 25) return false;
  if (!isLikelyEnglish(t)) return false;
  if (IRRELEVANT_HINTS.some(w => t.includes(w))) return false;

  // Must have at least one negative hint AND one product context word
  const hasNeg = NEGATIVE_HINTS.some(w => t.includes(w));
  const hasProd = PRODUCT_WORDS.some(w => t.includes(w));
  return hasNeg && hasProd;
}

async function main(){
  const manifest = JSON.parse(await fs.readFile(MANIFEST_PATH,"utf-8"));
  await fs.mkdir(DATA_DIR, { recursive: true });

  const indexPath = path.join(DATA_DIR,"index.json");
  let index = { latest:{}, files:[] };
  try { index = JSON.parse(await fs.readFile(indexPath,"utf-8")); } catch {}

  const days = manifest.recency_days ?? 30;
  const publishedAfter = new Date(Date.now() - days*86400*1000).toISOString();
  const vidLimit = manifest.limits?.videos_per_query ?? 6;
  const cmtLimit = manifest.limits?.comments_per_video ?? 150;

  for (const q of manifest.queries || []) {
    const topicId = q.id;
    const videoIds = new Set();
    const excludes = (q.exclude||[]).map(x=>x.toLowerCase());

    // 1) search recent videos per include term (English/US/date)
    for (const term of q.include || []) {
      const vids = await ytSearch(term, publishedAfter, vidLimit);
      for (const v of vids) videoIds.add(v);
      await sleep(200);
    }

    // 2) pull & filter comments
    const seenText = new Set();
    const items = [];
    for (const vid of videoIds) {
      const comments = await ytComments(vid, cmtLimit);
      for (const c of comments) {
        const text = (c.text || "").replace(/<[^>]+>/g,"").trim();
        const tnorm = normalizeText(text);

        // skip if video title/description likely matched an exclude (cheap client-side guard)
        if (excludes.some(x => tnorm.includes(x))) continue;

        if (!isRelevantComment(text)) continue;
        if (seenText.has(tnorm)) continue;   // de-dup by normalized text within snapshot
        seenText.add(tnorm);

        items.push({
          id: `${vid}:${c.id}`,
          text,
          likes: c.likes ?? 0,
          timestamp: c.publishedAt,
          lang: "en"
        });
      }
      if (items.length > 4000) break; // safety cap
      await sleep(150);
    }

    // 3) write snapshot and update index
    const snapshot = { source:"youtube", topic_id:topicId, fetched_at:new Date().toISOString(), items };
    const fname = `youtube_${topicId}_${dateStamp()}.json`;
    await fs.writeFile(path.join(DATA_DIR,fname), JSON.stringify(snapshot,null,2));
    index.latest[topicId] = fname;
    if (!index.files.includes(fname)) index.files.push(fname);
    console.log(`Wrote ${fname} with ${items.length} filtered comments`);
  }

  // 4) prune older snapshots & keep index clean
  await pruneOldSnapshots(DATA_DIR, RETAIN_DAYS, index);

  await fs.writeFile(indexPath, JSON.stringify(index,null,2));
}

async function ytSearch(q, publishedAfter, maxResults=6){
  const u = new URL("https://www.googleapis.com/youtube/v3/search");
  u.searchParams.set("part","id");
  u.searchParams.set("type","video");
  u.searchParams.set("q", q);
  u.searchParams.set("publishedAfter", publishedAfter);
  u.searchParams.set("maxResults", String(Math.min(50, maxResults)));
  u.searchParams.set("order","date");
  u.searchParams.set("regionCode", REGION);
  u.searchParams.set("relevanceLanguage", LANG);
  u.searchParams.set("key", API_KEY);
  const r = await fetch(u, { headers: { accept:"application/json" }});
  if (!r.ok) { console.error("YT search failed:", r.status, await r.text()); return []; }
  const j = await r.json();
  return (j.items||[]).map(i=>i.id?.videoId).filter(Boolean);
}

async function ytComments(videoId, max=150){
  const items = [];
  let pageToken = null;
  while (items.length < max) {
    const u = new URL("https://www.googleapis.com/youtube/v3/commentThreads");
    u.searchParams.set("part","snippet");
    u.searchParams.set("videoId", videoId);
    u.searchParams.set("maxResults","100");
    if (pageToken) u.searchParams.set("pageToken", pageToken);
    u.searchParams.set("key", API_KEY);
    const r = await fetch(u, { headers: { accept:"application/json" }});
    if (!r.ok) { console.error("YT comments failed:", r.status, await r.text()); break; }
    const j = await r.json();
    for (const it of j.items||[]) {
      const s = it.snippet.topLevelComment.snippet;
      items.push({
        id: it.id,
        text: s.textDisplay || "",
        likes: s.likeCount || 0,
        publishedAt: s.publishedAt
      });
      if (items.length >= max) break;
    }
    if (!j.nextPageToken) break;
    pageToken = j.nextPageToken;
  }
  return items;
}

async function pruneOldSnapshots(dir, retainDays, index){
  const files = await fs.readdir(dir);
  const keep = new Set();
  const now = new Date();

  for (const f of files) {
    const m = f.match(/^(youtube_.+?)_(\d{4}-\d{2}-\d{2})\.json$/);
    if (!m) { keep.add(f); continue; }
    const d = new Date(m[2]+"T00:00:00Z");
    const ageDays = (now - d) / 86400000;
    if (ageDays <= retainDays) keep.add(f);
    else {
      await fs.rm(path.join(dir,f)).catch(()=>{});
      console.log("Pruned old file:", f);
    }
  }

  // clean index.files if any removed
  index.files = index.files.filter(f => keep.has(f));
  // index.latest already points to newest per topic (just leave as-is)
}

const dateStamp = (d=new Date())=>{
  const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,"0"), da=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${da}`;
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

main().catch(e=>{ console.error(e); process.exit(1); });
