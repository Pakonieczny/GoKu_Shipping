#!/usr/bin/env python3
# Resolve merge conflicts in charm-nest-1.html that differ only in ?v= cache tokens: keep HEAD's token and append the other side's new suffix parts.
import re, sys

path = sys.argv[1]
lines = open(path).read().split('\n')
out, i, bad = [], 0, 0
tok = re.compile(r'\?v=([^"]+)"')
while i < len(lines):
    if not lines[i].startswith('<<<<<<< '):
        out.append(lines[i]); i += 1; continue
    j = lines.index('=======', i); k = next(n for n in range(j, len(lines)) if lines[n].startswith('>>>>>>> '))
    ours, theirs = lines[i + 1:j], lines[j + 1:k]
    if len(ours) != len(theirs):
        bad += 1; out.extend(lines[i:k + 1]); i = k + 1; continue
    merged = []
    for a, b in zip(ours, theirs):
        ma, mb = tok.search(a), tok.search(b)
        if a == b: merged.append(a); continue
        if not (ma and mb) or tok.sub('', a) != tok.sub('', b):
            merged = None; break
        pa, pb = ma.group(1).split('-'), mb.group(1).split('-')
        extra = [p for p in pb if p not in pa]
        merged.append(a.replace(ma.group(0), '?v=' + '-'.join(pa + extra) + '"', 1))
    if merged is None:
        bad += 1; out.extend(lines[i:k + 1])
    else:
        out.extend(merged)
    i = k + 1
open(path, 'w').write('\n'.join(out))
print('unresolved hunks:', bad)
