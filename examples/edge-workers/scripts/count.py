#!/usr/bin/env python3
"""Callers of one counter object: each POSTs /w/counter/<name> again once it has its answer, at most <rate> a
second (an account has 1000 requests a second, and they share one).
   count.py <gateway> <name> <seconds> <callers> <rate> <out.jsonl>
One line per request: when it was sent and answered (Unix seconds), its status, and the count it was told."""
import json, sys, threading, time, urllib.error, urllib.request

gateway, name, seconds, callers = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
rate, out = float(sys.argv[5]), sys.argv[6]
url, end, lock, lines = f"{gateway}/w/counter/{name}", time.time() + seconds, threading.Lock(), []


def caller():
    while time.time() < end:
        sent, count = time.time(), None
        try:
            # The gateway's deadline is 10 s: an answer later than this is the failure the checks look for.
            request = urllib.request.Request(url, method="POST", headers={"x-account": "counter"})
            with urllib.request.urlopen(request, timeout=15) as response:
                status, count = response.status, json.load(response).get("count")
        except urllib.error.HTTPError as e:
            status = e.code
        except Exception:
            status = 0
        with lock:
            lines.append({"sent": sent, "answered": time.time(), "status": status, "count": count})
        time.sleep(max(0.0, sent + 1 / rate - time.time()))


threads = [threading.Thread(target=caller) for _ in range(callers)]
for t in threads:
    t.start()
for t in threads:
    t.join()
with open(out, "w") as f:
    for line in sorted(lines, key=lambda line: line["sent"]):
        f.write(json.dumps(line) + "\n")
