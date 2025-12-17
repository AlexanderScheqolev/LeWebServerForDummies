# tiny_server.py
import socket, threading, time, json, os

HOST, PORT = '0.0.0.0', 8080
STATE_FILE, SNAP_FILE, HTML_FILE = 'state.json', 'snapshots.txt', 'index.html'

MASTER = True  # set False on followers
FOLLOWERS = ['http://127.0.0.1:8081', 'http://127.0.0.1:8082']  # set on master

vclock = {}  # {source: last_id}
last_snap_ts = 0
lock = threading.Lock()

def read_all(conn):
    data = b''
    conn.settimeout(1)
    try:
        while True:
            chunk = conn.recv(4096)
            if not chunk: break
            data += chunk
            if b'\r\n\r\n' in data:
                hdr, body = data.split(b'\r\n\r\n', 1)
                clen = 0
                for line in hdr.split(b'\r\n'):
                    if line.lower().startswith(b'content-length:'):
                        clen = int(line.split(b':',1)[1].strip())
                        break
                while len(body) < clen:
                    more = conn.recv(clen - len(body))
                    if not more: break
                    body += more
                return hdr.decode('latin-1'), body.decode('utf-8', 'ignore')
    except: pass
    return data.decode('latin-1'), ''

def http_resp(status, headers, body):
    msg = f'HTTP/1.1 {status}\r\n'
    for k,v in headers.items(): msg += f'{k}: {v}\r\n'
    return (msg + '\r\n' + body).encode('utf-8')

def load_state():
    if not os.path.exists(STATE_FILE): return {}
    try:
        with open(STATE_FILE,'r',encoding='utf-8') as f: return json.load(f)
    except: return {}

def save_state(state):
    with open(STATE_FILE,'w',encoding='utf-8') as f: json.dump(state,f,ensure_ascii=False)

def append_snapshot(payload_str):
    global last_snap_ts
    now = time.time()
    if now - last_snap_ts >= 300:  # 5 minutes
        with open(SNAP_FILE,'a',encoding='utf-8') as f:
            f.write(payload_str.strip() + '\n')
        last_snap_ts = now

def apply_patch(doc, patch):
    # Supports RFC6902 subset: add, remove, replace
    def get_parent_path(path):
        parts = [p for p in path.split('/') if p!='']
        parent = parts[:-1]; key = parts[-1] if parts else ''
        return parent, key
    def descend(obj, parts):
        cur = obj
        for p in parts:
            if isinstance(cur, list):
                idx = int(p); cur = cur[idx]
            else:
                cur = cur.get(p)
        return cur
    for op in (patch if isinstance(patch, list) else [patch]):
        typ = op.get('op'); path = op.get('path','/')
        parent_parts, key = get_parent_path(path)
        parent = doc if not parent_parts else descend(doc, parent_parts)
        if typ == 'add':
            val = op['value']
            if isinstance(parent, list): parent.insert(int(key), val)
            else: parent[key] = val
        elif typ == 'remove':
            if isinstance(parent, list): parent.pop(int(key))
            else: parent.pop(key, None)
        elif typ == 'replace':
            val = op['value']
            if isinstance(parent, list): parent[int(key)] = val
            else: parent[key] = val
        else:
            raise ValueError('unsupported op')
    return doc

def broadcast_put(body_json):
    import urllib.parse, http.client
    for url in FOLLOWERS:
        try:
            u = urllib.parse.urlsplit(url)
            conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=2)
            payload = json.dumps(body_json).encode('utf-8')
            conn.request('PUT','/replace',payload,{'Content-Type':'application/json','Content-Length':str(len(payload))})
            conn.getresponse().read()
            conn.close()
        except: pass

def handle_replace(body):
    req = json.loads(body)
    source = req['source']; rid = int(req['id']); payload_raw = req['payload']
    # payload can be string or object
    patch = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
    with lock:
        last = vclock.get(source, 0)
        if rid <= last:
            return 409, {'Content-Type':'application/json'}, json.dumps({'error':'stale id','last':last})
        state = load_state()
        try:
            new_state = apply_patch(state, patch)
        except Exception as e:
            return 400, {'Content-Type':'application/json'}, json.dumps({'error':str(e)})
        save_state(new_state)
        vclock[source] = rid
        append_snapshot(json.dumps(patch, ensure_ascii=False))
    if MASTER and FOLLOWERS:
        threading.Thread(target=broadcast_put, args=({'source':source,'id':rid,'payload':patch},), daemon=True).start()
    return 200, {'Content-Type':'application/json'}, json.dumps({'ok':True})

def serve(conn, addr):
    hdr, body = read_all(conn)
    line = hdr.split('\r\n',1)[0] if hdr else ''
    try:
        method, path, _ = line.split(' ')
    except:
        conn.sendall(http_resp('400 Bad Request', {'Content-Type':'text/plain'}, 'bad request'))
        conn.close(); return
    if method == 'PUT' and path == '/replace':
        code, headers, resp = handle_replace(body)
        conn.sendall(http_resp(f'{code} {"OK" if code==200 else "Error"}', headers, resp))
    elif method == 'GET' and path == '/get':
        state = load_state()
        conn.sendall(http_resp('200 OK', {'Content-Type':'application/json'}, json.dumps(state, ensure_ascii=False)))
    elif method == 'GET' and path == '/test':
        content = ''
        if os.path.exists(HTML_FILE):
            with open(HTML_FILE,'r',encoding='utf-8') as f: content = f.read()
        else:
            content = '<!doctype html><html><body><h1>index.html not found</h1></body></html>'
        conn.sendall(http_resp('200 OK', {'Content-Type':'text/html; charset=utf-8'}, content))
    elif method == 'GET' and path == '/vclock':
        with lock:
            payload = json.dumps(vclock, ensure_ascii=False)
        conn.sendall(http_resp('200 OK', {'Content-Type':'text/plain; charset=utf-8'}, payload))
    else:
        conn.sendall(http_resp('404 Not Found', {'Content-Type':'text/plain'}, 'not found'))
    conn.close()

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT)); s.listen(100)
    print(f'Listening on {HOST}:{PORT} (MASTER={MASTER})')
    while True:
        conn, addr = s.accept()
        threading.Thread(target=serve, args=(conn,addr), daemon=True).start()

if __name__ == '__main__':
    main()
