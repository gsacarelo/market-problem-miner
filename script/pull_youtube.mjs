
// Node 18+ (built-in fetch). No deps needed.
import { promises as fs } from "fs";
import path from "path";

const API_KEY = process.env.YT_API_KEY;
if(!API_KEY){
  console.error("Missing env YT_API_KEY");
  process.exit(1);
}

const MANIFEST_PATH = "config/topic_manifest.json";
const DATA_DIR = "docs/data";

async function main(){
  const manifest = JSON.parse(await fs.readFile(MANIFEST_PATH, "utf-8"));
  await fs.mkdir(DATA_DIR, { recursive: true });
  const indexPath = path.join(DATA_DIR, "index.json");
  let index = { latest:{}, files:[] };
  try{ index = JSON.parse(await fs.readFile(indexPath,"utf-8")); } catch{}

  const publishedAfter = new Date(Date.now() - (manifest.recency_days||30)*86400*1000).toISOString();

  for(const q of manifest.queries){
    const topicId = q.id;
    const videoIds = new Set();

    // 1) search recent videos per include term
    for(const term of q.include){
      const vids = await ytSearch(term, publishedAfter, manifest.limits?.videos_per_query || 6);
      vids.forEach(v=>videoIds.add(v));
      await sleep(200);
    }

    // 2) fetch comments for each video
    const maxPerVideo = manifest.limits?.comments_per_video || 120;
    const items = [];
    for(const vid of videoIds){
      const comments = await ytComments(vid, maxPerVideo);
      for(const c of comments){
        items.push({
          id: `${vid}:${c.id}`,
          text: sanitize(c.text),
          likes: c.likes ?? 0,
          timestamp: c.publishedAt,
          lang: "en"
          // no author/channel to avoid PII in public output
        });
      }
      if(items.length>4000) break; // safety cap
      await sleep(150);
    }

    const snapshot = {
      source: "youtube",
      topic_id: topicId,
      fetched_at: new Date().toISOString(),
      items
    };

    const fname = `youtube_${topicId}_${dateStamp()}.json`;
    await fs.writeFile(path.join(DATA_DIR, fname), JSON.stringify(snapshot,null,2));
    index.latest[topicId] = fname;
    if(!index.files.includes(fname)) index.files.push(fname);
    console.log(`Wrote ${fname} with ${items.length} comments`);
  }

  await fs.writeFile(indexPath, JSON.stringify(index,null,2));
}

async function ytSearch(q, publishedAfter, maxResults=6){
  const u = new URL("https://www.googleapis.com/youtube/v3/search");
  u.searchParams.set("part","id");
  u.searchParams.set("type","video");
  u.searchParams.set("q", q);
  u.searchParams.set("publishedAfter", publishedAfter);
  u.searchParams.set("maxResults", String(Math.min(50,maxResults)));
  u.searchParams.set("key", API_KEY);
  const r = await fetch(u, { headers: { "accept":"application/json" }});
  const j = await r.json();
  return (j.items||[]).map(i=>i.id?.videoId).filter(Boolean);
}

async function ytComments(videoId, max=120){
  const items = [];
  let pageToken=null;
  while(items.length<max){
    const u = new URL("https://www.googleapis.com/youtube/v3/commentThreads");
    u.searchParams.set("part","snippet");
    u.searchParams.set("videoId", videoId);
    u.searchParams.set("maxResults","100");
    if(pageToken) u.searchParams.set("pageToken", pageToken);
    u.searchParams.set("key", API_KEY);
    const r = await fetch(u, { headers: { "accept":"application/json" }});
    const j = await r.json();
    for(const it of (j.items||[])){
      const s = it.snippet.topLevelComment.snippet;
      items.push({
        id: it.id,
        text: (s.textDisplay||"").replace(/<[^>]+>/g,""),
        likes: s.likeCount||0,
        publishedAt: s.publishedAt
      });
      if(items.length>=max) break;
    }
    if(!j.nextPageToken) break;
    pageToken = j.nextPageToken;
  }
  return items;
}

const sanitize = s => (s||"").replace(/\s+/g," ").trim();
const dateStamp = (d=new Date())=>{
  const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,"0"), da=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${da}`;
}
const sleep = ms => new Promise(r=>setTimeout(r,ms));

main().catch(e=>{ console.error(e); process.exit(1); });
