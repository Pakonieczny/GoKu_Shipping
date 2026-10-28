#!/bin/bash
# what the lead is running right now, for the board's "Claude (lead)" row (edit RUN/WHAT when the run changes)
SP=/tmp/claude-0/-home-user/41f0c21f-5485-5cfe-8e7d-8dab8957f410/scratchpad
RUN=/dev/null
WHAT="Shipped d58eea9 = Design & Engrave batch 1 (editor extension points, live readouts, Ctrl/Alt centre resize, smart guides; Blend + Warp + Distort; image trace) to main, verifying live. Merging the next workstreams into the integration branch as they hand back."
DONE=$(grep -c " exit " $RUN 2>/dev/null); FAIL=$(grep " exit [1-9]" $RUN 2>/dev/null | awk '{print $1}' | tr '\n' ' ')
echo "$WHAT · tests finished: ${DONE:-0}/11${FAIL:+ · failed so far: $FAIL} · last: $(tail -1 $RUN 2>/dev/null)" > $SP/lead-now.txt
