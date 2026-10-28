# tokbump.py <repo> <old> <new>: every served .js/.css file changed between two commits must have a different ?v= token where the pages reference it (the service worker caches by token)
import re, subprocess, sys
repo, old, new = sys.argv[1:4]
g = lambda *a: subprocess.run(["git", "-C", repo, *a], capture_output=True, text=True).stdout
changed = [f for f in g("diff", "--name-only", old, new).split() if re.search(r'\.(js|css)$', f) and not f.startswith(('tests/', 'netlify/', 'scripts/'))]
REFS = ["charm-nest-1.html", "charm-nest-bridge.js", "charm-nest-efficiency.js"]
def refs(ref):
    out = {}
    for f in REFS:
        for name, t in re.findall(r'([\w./-]+\.(?:js|css))\?v=([\w.-]+)', g("show", f"{ref}:{f}")): out.setdefault(name.split('/')[-1], set()).add(t)
    return out
A, B = refs(old), refs(new); bad = 0
for f in changed:
    n = f.split('/')[-1]
    if n not in B: print(f"  (no ?v= reference found for {f})"); continue
    if A.get(n) == B[n]: bad += 1; print(f"NOT BUMPED: {f} still {sorted(B[n])}")
print("not bumped:", bad, "of", len(changed))
