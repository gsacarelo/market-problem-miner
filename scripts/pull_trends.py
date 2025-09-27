# Python 3.11 on GitHub Actions; pip: pytrends
from pytrends.request import TrendReq
from datetime import datetime, timedelta
import json, os

MANIFEST_PATH = "config/topic_manifest.json"
DATA_DIR = "docs/data"
RETAIN_DAYS = int(os.getenv("RETAIN_DAYS", "14"))

def date_stamp(dt=None):
    d = (dt or datetime.utcnow())
    return f"{d.year}-{d.month:02d}-{d.day:02d}"

def load_manifest():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def prune_old_files():
    import re, glob, os, time
    keep = set()
    now = time.time()
    for fp in glob.glob(os.path.join(DATA_DIR, "trends_*.json")):
        m = re.search(r"_(\d{4}-\d{2}-\d{2})\.json$", fp)
        if not m: keep.add(fp); continue
        # age by file date in name
        from datetime import datetime
        d = datetime.strptime(m.group(1), "%Y-%m-%d")
        age_days = (datetime.utcnow() - d).days
        if age_days <= RETAIN_DAYS:
            keep.add(fp)
        else:
            try: os.remove(fp); print("Pruned old:", fp)
            except: pass
    # index.json is managed by node scripts; no changes here

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    manifest = load_manifest()
    recency_days = manifest.get("recency_days", 30)

    pytrends = TrendReq(hl="en-US", tz=360)  # US English, UTC+6 offset

    for q in manifest.get("queries", []):
        topic_id = q["id"]
        include = q.get("include", [])
        items = []

        for term in include:
            # interest over time (last 90 days)
            pytrends.build_payload([term], timeframe="today 3-m", geo="US")
            try:
                iot = pytrends.interest_over_time()
            except Exception as e:
                print("IOT error for", term, e)
                iot = None

            if iot is not None and not iot.empty:
                # Use last 14 days avg as "current" signal
                last_vals = iot[term].tail(14).tolist()
                avg14 = sum(last_vals)/max(1,len(last_vals))
                items.append({
                    "id": f"{topic_id}:iot:{term}",
                    "text": f"TRENDS: interest over time average (14d) for '{term}' is {round(avg14,1)}",
                    "timestamp": datetime.utcnow().isoformat()+"Z",
                    "lang": "en"
                })

            # related queries (rising)
            try:
                rq = pytrends.related_queries()
                rising = rq.get(term, {}).get("rising")
                if rising is not None:
                    for _, row in rising.head(10).iterrows():
                        kw = str(row["query"])
                        val = int(row["value"]) if not (row["value"] != row["value"]) else 0
                        items.append({
                            "id": f"{topic_id}:rq:{term}:{kw}",
                            "text": f"RISING QUERY: {kw} (vs '{term}', score +{val})",
                            "timestamp": datetime.utcnow().isoformat()+"Z",
                            "lang": "en"
                        })
            except Exception as e:
                print("RQ error for", term, e)

        snap = {
            "source": "trends",
            "topic_id": topic_id,
            "fetched_at": datetime.utcnow().isoformat()+"Z",
            "items": items
        }
        out = os.path.join(DATA_DIR, f"trends_{topic_id}_{date_stamp()}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
        print(f"Wrote {out} with {len(items)} items")

    prune_old_files()

if __name__ == "__main__":
    main()
