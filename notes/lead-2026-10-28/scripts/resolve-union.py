#!/usr/bin/env python3
# resolve-union.py <file>...: resolves merge conflicts where both sides only ADDED or re-tokened lines in a list (script tags in an HTML page,
# entries in scripts/build-public.cjs): every line of both sides is kept once, in order (ours first, then theirs' new lines placed after
# the line they followed); two versions of the same <script src="x.js?v=..."> keep ONE tag with both sides' ?v= suffix parts. A hunk where
# the same key appears with other differences is left as a conflict (printed).
import re, sys

tok = re.compile(r'\?v=([^"]+)"')
src = re.compile(r'<script src="([^"?]+)')


def key(line):
    m = src.search(line)
    return ('src', m.group(1)) if m else ('line', line.strip())


def merge_tag(a, b):
    ma, mb = tok.search(a), tok.search(b)
    if not (ma and mb): return a if a == b else None
    pa, pb = ma.group(1).split('-'), mb.group(1).split('-')
    merged = a.replace(ma.group(0), '?v=' + '-'.join(pa + [p for p in pb if p not in pa]) + '"', 1)
    rest = lambda s: tok.sub('', s)
    return merged if rest(a) == rest(b) else None


for path in sys.argv[1:]:
    lines = open(path).read().split('\n'); out = []; i = 0; bad = 0
    while i < len(lines):
        if not lines[i].startswith('<<<<<<< '):
            out.append(lines[i]); i += 1; continue
        j = next(n for n in range(i, len(lines)) if lines[n] == '=======' or lines[n].startswith('||||||| '))
        k = next(n for n in range(j, len(lines)) if lines[n].startswith('>>>>>>> '))
        sep = next(n for n in range(j, k) if lines[n] == '=======')
        ours, theirs = lines[i + 1:j], lines[sep + 1:k]
        res = list(ours); idx = {key(l): n for n, l in enumerate(res)}; ok = True; last = -1
        for t in theirs:
            kt = key(t)
            if kt in idx:
                m = merge_tag(res[idx[kt]], t) if kt[0] == 'src' else (res[idx[kt]] if res[idx[kt]].strip() == t.strip() else None)
                if m is None: ok = False; break
                res[idx[kt]] = m; last = idx[kt]
            else:
                res.insert(last + 1, t); idx = {key(l): n for n, l in enumerate(res)}; last = idx[kt]
        if ok: out.extend(res)
        else: bad += 1; out.extend(lines[i:k + 1]); print(f'{path}: unresolved hunk at line {i + 1}')
        i = k + 1
    open(path, 'w').write('\n'.join(out))
    print(f'{path}: unresolved {bad}')
