/* charm-nest-instant.js — INSTANT (Paul, 23 Nov 2026: "nowhere in the application is the system allowed to make the user wait"). One way to press: the screen changes now (show), the cloud is asked behind it (send),
   presses of one thing are sent in order and never refused as busy; done() once the cloud did it, back() puts the screen back when it refused, and why is said in red with Try again.
   Nothing is stored on the computer: a press lives in this page's memory until the cloud answers.
     CNInstant.press({ key, what, show, send, done, back, again: false | fn, quiet }) → { done: Promise<{ ok, answer?, error? }> }      CNInstant.pending(key) → presses not answered yet */
(function (root) {
  "use strict";
  const lanes = new Map(), count = new Map(); let seq = 0;
  const words = e => String((e && e.data && e.data.error) || (e && (e.error || e.message)) || e || "the cloud did not answer").replace(/^Error:\s*/, "").replace(/[.\s]+$/, "");
  const safe = (f, ...a) => { try { return f ? f(...a) : undefined; } catch (e) { console.warn("[instant]", e); } };
  function said(text, again) {
    const T = (root.CN && root.CN.toast) || root.toast, el = T && T(text, "bad", 12000); if (!again || !el || !el.appendChild) return;
    const b = root.document.createElement("button"); b.type = "button"; b.className = "btn xs"; b.textContent = "Try again"; b.onclick = () => { b.disabled = true; safe(el._go); again(); }; el.appendChild(b);
  }
  function press(o) {
    const key = o.key || "p" + (++seq), token = safe(o.show), before = lanes.get(key); count.set(key, (count.get(key) || 0) + 1);
    const work = (before || Promise.resolve()).then(() => o.send(token)).then(a => {
      if (!a || a.ok === false || a.error) throw Object.assign(new Error(words(a && (a.error || a.why || a.text))), { answer: a });
      safe(o.done, a, token); return { ok: true, answer: a };
    }).catch(e => {
      safe(o.back, token, e);
      if (!o.quiet && !e.told) said(`${o.what || "Not saved"}: ${words(e)}.`, o.again === false ? null : o.again || (() => press(o)));
      return { ok: false, error: words(e), answer: e.answer };
    });
    const last = work.then(() => { const n = count.get(key) - 1; if (n) count.set(key, n); else count.delete(key); if (lanes.get(key) === last) lanes.delete(key); });
    lanes.set(key, last); return { key, done: work };
  }
  root.CNInstant = { press, pending: key => count.get(key) || 0 };
})(typeof window !== "undefined" ? window : globalThis);
