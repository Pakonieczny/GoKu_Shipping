# keeps the agent board's rows (agents.json) — usage: python3 board.py set <id> key=value ...   |  python3 board.py add <json>
import json, sys, datetime, os
P = os.path.join(os.path.dirname(__file__), 'agents.json')
rows = json.load(open(P)) if os.path.exists(P) else []
now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
cmd = sys.argv[1]
if cmd == 'add':
    r = json.loads(sys.argv[2]); r.setdefault('updated', now); rows = [x for x in rows if x['id'] != r['id']] + [r]
elif cmd == 'set':
    r = next(x for x in rows if x['id'] == sys.argv[2])
    for kv in sys.argv[3:]:
        k, v = kv.split('=', 1)
        if v == 'NOW': v = now
        elif k in ('tests_passed', 'tests_failed', 'tests_removed', 'fixes', 'order'): v = int(v)
        r[k] = v
    r['updated'] = now
json.dump(rows, open(P, 'w'), indent=1)
print(len(rows), 'rows')
