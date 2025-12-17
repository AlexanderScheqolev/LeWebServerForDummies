# tiny_client.py
import http.client, json, urllib.parse

SERVER = "127.0.0.1:8080"   # адрес мастера или фолловера

def put_replace(source, rid, patch):
    conn = http.client.HTTPConnection(*SERVER.split(":"))
    body = json.dumps({"source": source, "id": rid, "payload": json.dumps(patch)})
    conn.request("PUT", "/replace", body,
                 {"Content-Type": "application/json", "Content-Length": str(len(body))})
    resp = conn.getresponse()
    print("PUT /replace:", resp.status, resp.read().decode())
    conn.close()

def get_state():
    conn = http.client.HTTPConnection(*SERVER.split(":"))
    conn.request("GET", "/get")
    resp = conn.getresponse()
    print("GET /get:", resp.status, resp.read().decode())
    conn.close()

def get_vclock():
    conn = http.client.HTTPConnection(*SERVER.split(":"))
    conn.request("GET", "/vclock")
    resp = conn.getresponse()
    print("GET /vclock:", resp.status, resp.read().decode())
    conn.close()

def get_test():
    conn = http.client.HTTPConnection(*SERVER.split(":"))
    conn.request("GET", "/test")
    resp = conn.getresponse()
    print("GET /test:", resp.status)
    print(resp.read().decode())
    conn.close()

if __name__ == "__main__":
    # пример использования
    patch = {"op": "add", "path": "/hello", "value": {"name": "Ginger"}}
    put_replace("Shchegolev", 1, patch)
    get_state()
    get_vclock()
    get_test()
