#!/bin/bash
# slot.sh <command...>: runs the command holding one of this machine's 3 browser-test slots (4 cores, ~20 agents).
# First come, first served: waiters stand in one line (a blocking lock); the head of the line takes the next free slot.
# SLOT_PRIO=1 (urgent bugs, the lead's pre-ship runs) skips the line. Put any time limit INSIDE: slot.sh timeout 600 node tests/...
L=/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad/locks
export PW_DIR=${PW_DIR:-/opt/node-tools/node_modules} CHROMIUM=${CHROMIUM:-/opt/pw-browsers/chromium-1243/chrome-linux64/chrome}
q=""
if [ -z "$SLOT_PRIO" ]; then exec {q}>"$L/line"; flock "$q"; fi
while true; do
  for i in 1 2 3; do
    exec {fd}>"$L/slot$i"
    if flock -n "$fd"; then
      [ -n "$q" ] && { flock -u "$q"; exec {q}>&-; }
      timeout -k 10 "${SLOT_MAX:-600}" "$@"; rc=$?; [ $rc = 124 ] && echo "slot.sh: stopped after ${SLOT_MAX:-600} s (a test holds a slot at most 10 min; SLOT_MAX=<s> for a longer one)" >&2; flock -u "$fd"; exec {fd}>&-; exit $rc
    fi
    exec {fd}>&-
  done
  sleep 1
done
