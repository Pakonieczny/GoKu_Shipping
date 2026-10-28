#!/usr/bin/env python3
# Fills the board's rows (board/agents.json) with each live agent's real activity: state, minutes since its last step, commits, files changed,
# what it is doing now. Run every 3 minutes by the monitor; the lead then publishes agents.json to the board.
import json, os, subprocess, time, datetime, re

SP = "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad"
TASKS = "/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/tasks"
B = SP + "/board/agents.json"
# agent id -> (board row id, worktree)
AG = {
    "a42236c45a7daeff2": ("local-store-p1", "wt-store"),
    "abd35231dd32fd1d6": ("de-feedback", "wt-de-feedback"), "a909d6b516f6d76c5": ("de-rulers", "wt-de-rulers"),
    "a925a9f2929572a8f": ("de-pen", "wt-de-pen"), "ab5872e120f01d306": ("de-warp", "wt-de-warp"), "a3ce05d237db781e5": ("de-ops", "wt-de-ops"),
    "ac01718c1a8ad6843": ("de-trace", "wt-de-trace"), "ac4cbd9c035695d58": ("de-image", "wt-de-image"), "ae828a5d729f6efa1": ("de-text", "wt-de-text"),
    "ab275f8ca5c1e80d3": ("de-vimport", "wt-de-vimport"), "af114bb2ccb5f706c": ("de-designs", "wt-de-designs"), "acc9d6120b4748ee1": ("de-library", "wt-de-library"),
    "acbce20e63df93ae2": ("de-masters", "wt-de-masters"), "a6ecf232b267d8e9d": ("de-sendsheet", "wt-de-sendsheet"), "a4d3307fda8a88af0": ("de-custompool", "wt-de-custompool"),
    "a28303212b071279b": ("de-tab", "wt-de-tab"), "aae0c82c2db2e93d9": ("de-emplog", "wt-de-emplog"), "a7764b20331f1122f": ("de-refit", "wt-de-refit"),
    "ab57244fabf439529": ("fix-approve", "wt-fix-approve"), "a2200fb3aa0ebf322": ("fix-seals", "wt-fix-seals"),
}
EXTRA = os.path.join(SP, "board-live-extra.json")   # agents added later: {"<agent id>": ["<row id>", "<worktree dir>"]}
if os.path.exists(EXTRA):
    try: AG.update({k: tuple(v) for k, v in json.load(open(EXTRA)).items()})
    except Exception: pass


def tail_events(path, size=300_000):
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


def git(wt, *a):
    try: return subprocess.run(["git", "-C", wt, *a], capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception: return ""


def clean(s, n):
    s = re.sub(r"/tmp/claude-0/[^ ]*/scratchpad/", "", str(s or "")); s = re.sub(r"\s+", " ", s).strip()
    return s[:n]


now = time.time(); stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'); hm = time.strftime("%H:%M", time.gmtime(now))
rows = json.load(open(B)); byid = {r["id"]: r for r in rows}
for aid, (rid, wt) in AG.items():
    r = byid.get(rid)
    if not r: continue
    ev = tail_events(os.path.join(TASKS, aid + ".output"))
    if not ev: continue
    last_t, text, tool, done = None, "", "", False
    for e in ev:
        t = ts(e)
        if t: last_t = t
        m = e.get("message") or {}
        if e.get("type") == "assistant":
            for c in m.get("content", []) or []:
                if c.get("type") == "text" and c.get("text", "").strip(): text = c["text"].strip()
                if c.get("type") == "tool_use":
                    inp = c.get("input") or {}
                    tool = str(inp.get("description") or inp.get("command") or inp.get("file_path") or c.get("name", ""))
                    done = c.get("name") == "SubagentHandback"
    w = os.path.join(SP, wt) if wt else ""
    ahead = git(w, "rev-list", "--count", "origin/main..HEAD") if w and os.path.isdir(w) else ""
    dirty = git(w, "status", "--porcelain") if w and os.path.isdir(w) else ""
    nd = len([l for l in dirty.splitlines() if l and not l.endswith("node_modules")])
    idle = int((now - last_t) // 60) if last_t else None
    if "summary" not in r: r["summary"] = r.get("note", "")
    if done:
        if r.get("status") not in ("done", "failed"): r["status"] = "done"
        if not r.get("finished"): r["finished"] = stamp
        live = f"Handed back {time.strftime('%H:%M', time.gmtime(last_t)) if last_t else ''} UTC · {ahead or 0} commits"
    else:
        r["status"] = "blocked" if idle is not None and idle >= 10 else "running"
        live = f"LIVE {hm} UTC · last step {idle if idle is not None else '?'} min ago · {ahead or 0} commits · {nd} files changed · now: {clean(tool, 90)}" + (f" · says: {clean(text, 110)}" if text else "")
    r["note"] = live + (" ‖ " + r["summary"] if r.get("summary") else "")
    r["updated"] = stamp
# normalise statuses the board does not know
for r in rows:
    if r.get("status") == "testing": r["status"] = "running"
    if r.get("status") == "waiting": r["status"] = "queued"
# the lead's own background runs (integration tests): one row
lead = byid.get("lead-integration")
if lead is None:
    lead = {"id": "lead-integration", "order": 100, "agent": "Claude (lead)", "phase": "Integration", "scope": "Merging finished branches, testing them together through the test queue, shipping to main and checking the live site", "runs_on": "Cloud session", "status": "running", "started": stamp, "tests_passed": 0, "tests_failed": 0, "tests_removed": 0, "fixes": 0}
    rows.append(lead)
lf = os.path.join(SP, "lead-now.txt")
lead["note"] = (open(lf).read().strip() if os.path.exists(lf) else "") ; lead["updated"] = stamp
json.dump(rows, open(B, "w"), indent=1)
print("board rows", len(rows), "running", sum(r.get("status") == "running" for r in rows), "done", sum(r.get("status") == "done" for r in rows))
