/* The ENGRAVE tab shows one ROW per earring order line (Paul, 10 Oct 2026: "All earring sets (Stud and Huggie Hoop) should be listed as one order with 2
 * individual thumbnails or the 1 thumbnail we currently have with both left and right charm vectors displayed side by side").
 *
 * This is presentation and navigation only. Underneath, the engraving is still one JOB per piece slot (charm-nest-engrave-sides.js): a Left job and a Right
 * job, each with its own words, fit, approval, seal and back file. A row is the Left job and the Right job of ONE order line put side by side; nothing
 * here reads or writes a record. Every other job stays a row of its own, exactly as before: a single charm, a single earring, a disc (D1..Dn), a necklace,
 * a plain quantity-N line (all have no ear, so nothing groups).
 *
 * Quantity: a pair line with quantity 2 has four pieces (L R L R) but still two jobs (the Left job holds both left pieces, the Right job both right pieces),
 * so it is ONE row ("2 pairs", each ear marked x2), as the Orders list shows it ("Qty 2 · Left + Right").
 *
 * A row lives in the tab whose jobs it holds: Placements holds the ears still to settle, Decided the ears decided. When one ear is decided and the other is
 * not, the line is in both tabs, each with the ear it holds (a row of one ear reads as a single piece's row does today).
 *
 *   earOf(job)                    "L" | "R" | null           the ear a job is for (null: not an ear, never grouped)
 *   lineKeyOf(job)                the order line's key       (job.rowKey, else the key without its "#slot")
 *   group(selected, universe)     [row]                       rows in the order of `selected`; each row { key, lineKey, jobs:[L?, R?], lead, pair }.
 *                                                             `universe` holds the jobs a row may take its other ear from (the same tab's jobs, before any search or date filter)
 *   rowOf(rows, jobKey)           the row that holds the job, or null
 *   neighbour(rows, jobKey, dir)  the row after (dir 1) or before (dir -1), wrapping; for a key not in a row: the first (1) or the last (-1)
 *   openLead(row, busy)           the job a click on the row opens: the first ear not busy (else the first)
 *   counts(queueRows, decidedRows) { decided, remaining, n, of, text }   "N of M · K done" counted in rows
 *   wordsOf(job)                  the words as one line ("Anna / Ben")
 *   sameWords(a, b)               the same words on both ears (exact lines; empty words are never "the same")
 *   stageOf(job, busy)            what an ear is waiting for, in plain words
 *   approveBoth(row, o)           { ok, why }: may both ears be approved with one press (see below)
 *
 * Approving "the row": each ear keeps its own Approve button, exactly as before. The row offers ONE extra press, "Approve both ears", only when
 *   (1) the row holds a Left and a Right, both ready to approve (state review, a fit that passed its check, nothing running on either),
 *   (2) both carry the very same words (a person never approves words that differ without looking at them: ears whose words differ are approved one by one), and
 *   (3) the placement of each ear, as it stands now, has been put in front of the person (o.shown(job)).
 * The press then runs the ordinary approval of the Left, and only when it settled, the ordinary approval of the Right: two approvals, two seals, two back
 * files, two timeline events, as if each button had been pressed.
 * Pure: no page, no network, no clock. Loads in the page (window.CharmNestEngraveRows) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveRows = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const KEY = /^(.*)#(L|R|D\d{1,2})$/;
  const slotOf = job => { if (!job) return null; if (job.slot) return String(job.slot); const m = KEY.exec(String(job.key == null ? '' : job.key)); return m ? m[2] : null; };
  const earOf = job => { const s = slotOf(job); return s === 'L' || s === 'R' ? s : null; };
  const lineKeyOf = job => { if (!job) return ''; if (job.rowKey) return String(job.rowKey); const m = KEY.exec(String(job.key == null ? '' : job.key)); return m ? m[1] : String(job.key == null ? '' : job.key); };
  const mkRow = (lineKey, jobs) => ({ key: jobs[0].key, lineKey, jobs, lead: jobs[0], pair: jobs.length === 2 });

  /** Rows, in the order of `selected`: the Left and the Right job of one line are one row, at the place of whichever came first. */
  function group(selected, universe) {
    const uni = Array.isArray(universe) ? universe : selected || [], ears = new Map();
    for (const j of uni) { const s = earOf(j); if (!s) continue; const k = lineKeyOf(j); if (!ears.has(k)) ears.set(k, {}); if (!ears.get(k)[s]) ears.get(k)[s] = j; }
    const out = [], done = new Set();
    for (const j of selected || []) {
      const s = earOf(j), k = lineKeyOf(j);
      if (!s) { out.push(mkRow(k, [j])); continue; }
      if (done.has(k)) continue; done.add(k);
      const e = Object.assign({}, ears.get(k) || {}); if (!e[s]) e[s] = j;
      out.push(mkRow(k, ['L', 'R'].filter(x => e[x]).map(x => e[x])));
    }
    return out;
  }
  const rowOf = (rows, jobKey) => (rows || []).find(r => r.jobs.some(j => j.key === jobKey)) || null;
  function neighbour(rows, jobKey, dir) {
    const list = rows || []; if (!list.length) return null;
    const i = list.findIndex(r => r.jobs.some(j => j.key === jobKey));
    if (i < 0) return dir < 0 ? list[list.length - 1] : list[0];
    return list[(i + (dir < 0 ? list.length - 1 : 1)) % list.length];
  }
  const openLead = (row, busy) => (row ? (row.jobs.find(j => !(busy && busy(j))) || row.jobs[0]) : null);
  function counts(queueRows, decidedRows) {
    const decided = (decidedRows || []).length, remaining = (queueRows || []).length;
    return { decided, remaining, n: decided + 1, of: decided + remaining, text: `${decided + 1} of ${decided + remaining} · ${decided} done` };
  }
  const wordsOf = job => ((job && job.lines) || []).map(s => String(s).trim()).filter(Boolean).join(' / ');
  const sameWords = (a, b) => { const x = wordsOf(a), y = wordsOf(b); return !!x && x === y; };
  function stageOf(job, busy) {
    if (!job) return '';
    if (busy) return job.state === 'classify' ? 'Reading words…' : 'Preparing preview…';
    switch (job.state) {
      case 'classify': return 'Reading words…';
      case 'words': case 'blocked': return 'Words to confirm';
      case 'ready': case 'fitting': return 'Preparing preview…';
      case 'review': return 'Placement to check';
      case 'approved': case 'written': return 'Approved';
      case 'skipped': return 'No engraving';
      default: return '';
    }
  }
  const settling = j => !!(j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving);
  function approveBoth(row, o) {
    o = o || {};
    if (!row || !row.pair) return { ok: false, why: 'This line has one ear to approve.' };
    const [l, r] = row.jobs;
    if (!row.jobs.every(j => j.state === 'review' && j.fit && j.verify && j.verify.geometry && j.verify.geometry.ok)) return { ok: false, why: 'Both ears must have a placement to check first.' };
    if (row.jobs.some(j => settling(j) || (o.busy && o.busy(j)))) return { ok: false, why: 'An ear is still being prepared or approved.' };
    if (!sameWords(l, r)) return { ok: false, why: 'The words differ between the ears: approve each ear on its own.' };
    for (const j of row.jobs) if (o.shown && !o.shown(j)) return { ok: false, why: `Look at the ${j === l ? 'Left' : 'Right'} ear's placement first (it is not on screen yet).` };
    return { ok: true, why: '' };
  }
  return { earOf, lineKeyOf, group, rowOf, neighbour, openLead, counts, wordsOf, sameWords, stageOf, approveBoth };
});
