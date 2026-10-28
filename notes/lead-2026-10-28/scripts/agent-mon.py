#!/usr/bin/env python3
# One line per running agent: idle time, what it last did, code changed/committed. Reads only the tail of each transcript.
import json, os, subprocess, sys, time

TASKS = "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/tasks"
WT = "/home/user/goku_shipping/.claude/worktrees"
AGENTS = [
    ("a42236c45a7daeff2", "Local store (Phase 1)", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-store"),
    ("aa6569d65415c6c37", "Bar orders waiting -> front", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-bar-waiting"),
    ("a909d6b516f6d76c5", "DE rulers", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-rulers"),
    ("a925a9f2929572a8f", "DE pen", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-pen"),
    ("a2ce3dc0daa28facb", "Navigation round 4", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-nav4"),
    ("a3ce05d237db781e5", "DE ops", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-ops"),
    ("a7883fa11af818737", "Local-first Phase 4 (service worker)", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-sw"),
    ("ac4cbd9c035695d58", "DE image", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-image"),
    ("ae828a5d729f6efa1", "DE text", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-text"),
    ("af959c47001f89f4c", "FIX approve after hold/release", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-fix-held"),
    ("af114bb2ccb5f706c", "DE designs", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-designs"),
    ("acc9d6120b4748ee1", "DE library", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-library"),
    ("acbce20e63df93ae2", "DE masters", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-masters"),
    ("a6ecf232b267d8e9d", "DE sendsheet", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-sendsheet"),
    ("a4d3307fda8a88af0", "DE custompool", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-custompool"),
    ("a28303212b071279b", "DE tab", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-tab"),
    ("aae0c82c2db2e93d9", "DE emplog", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-emplog"),
    ("a7764b20331f1122f", "DE refit", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-refit"),
    ("a5f64f2a38f32df82", "DE extras", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-de-extras"),
    ("a2200fb3aa0ebf322", "FIX seals", "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/wt-fix-seals"),
]
extra = sys.argv[1:]
AGENTS += [tuple(a.split("=", 1)) + ("",) for a in extra]


def tail_events(path, size=400_000):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); n = f.tell(); f.seek(max(0, n - size)); data = f.read().decode("utf8", "replace")
    except OSError:
        return []
    out = []
    for line in data.splitlines()[1:]:
        try: out.append(json.loads(line))
        except Exception: pass
    return out


def ts(e):
    t = e.get("timestamp")
    if not t: return None
    try: return time.mktime(time.strptime(t[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
    except Exception: return None


def git(wt, *args):
    try: return subprocess.run(["git", "-C", wt, *args], capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception: return ""


now = time.time()
for aid, name, wtp in AGENTS:
    ev = tail_events(os.path.join(TASKS, aid + ".output"))
    last_t, text, tool, done = None, "", "", False
    for e in ev:
        t = ts(e)
        if t: last_t = t
        m = e.get("message") or {}
        if e.get("type") == "assistant":
            for c in m.get("content", []) or []:
                if c.get("type") == "text" and c.get("text", "").strip(): text = c["text"].strip().replace("\n", " ")
                if c.get("type") == "tool_use":
                    inp = c.get("input") or {}
                    tool = c.get("name", "") + ": " + str(inp.get("description") or inp.get("command") or inp.get("file_path") or "")[:90]
                    done = c.get("name") == "SubagentHandback"   # (a resumed agent works again after its hand-back)
    wt = wtp or os.path.join(WT, "agent-" + aid)
    ahead = git(wt, "rev-list", "--count", "origin/main..HEAD") if os.path.isdir(wt) else "-"
    dirty = git(wt, "status", "--porcelain") if os.path.isdir(wt) else ""
    nd = len([l for l in dirty.splitlines() if l and not l.endswith("node_modules")])
    idle = f"{int((now - last_t) // 60)}m" if last_t else "?"
    state = "DONE" if done else ("IDLE!" if last_t and now - last_t > 600 else "busy")
    print(f"[{name}] {state} idle {idle} · commits {ahead} · files changed {nd} · last: {tool[:110]} · says: {text[:150]}")
