# tokcheck.py <repo> <head> <branch-ref>...: for every "<file>?v=<token>" in the served pages and the bridge, every suffix part a merged branch's token has must be in HEAD's token for that file
import re, subprocess, sys
repo, head, refs = sys.argv[1], sys.argv[2], sys.argv[3:]
FILES = ["charm-nest-1.html", "charm-nest-bridge.js", "charm-nest-efficiency.js", "charm-nest-sw.js"]
pat = re.compile(r'([\w./-]+\.(?:js|css|json|png|webp|woff2?))\?v=([\w.-]+)')
def toks(ref):
    out = {}
    for f in FILES:
        try: s = subprocess.run(["git", "-C", repo, "show", f"{ref}:{f}"], capture_output=True, text=True).stdout
        except Exception: continue
        for name, t in pat.findall(s): out.setdefault((f, name), set()).update(t.split('-'))
    return out
H = toks(head); bad = 0
for r in refs:
    for k, parts in toks(r).items():
        if k not in H: continue
        miss = parts - H[k]
        if miss: bad += 1; print(f"{r}: {k[0]} {k[1]} missing {sorted(miss)}")
print("missing:", bad)
