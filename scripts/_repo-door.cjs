/*  scripts/_repo-door.cjs — the Charm Repo's password door, for the local scripts (index-master.cjs, catalog-repair.cjs).
 *  Uploading to, patching, deleting from or downloading out of the Charm Repo needs a token that the site gives for Paul's login
 *  password (netlify/functions/_charmNestRepoAuth.js; plans/repo-protect-1011/REPOAUTH-api.md). A script asks for it itself:
 *    · the password comes from the environment variable CHARM_REPO_PASSWORD, or is typed at the terminal (hidden); it is sent only to
 *      the site's repoUnlock, and is never printed, written to a file or kept past this process;
 *    · one upload token (15 minutes) covers every upload and patch, and is asked for again when it runs out;
 *    · a delete asks again for every delete, as the site does (each unlock is an alert on Paul's screen);
 *    · every try is recorded on the site like any other.
 *  An environment with no password and no terminal stops with one plain line instead of a 401. */
"use strict";
const RepoAuth = require("../netlify/functions/_charmNestRepoAuth.js");   // (the tables only: this file loads no firebase module)

let password = process.env.CHARM_REPO_PASSWORD || "";
const held = new Map();   // action → { token, exp }

function ask(q) {
  return new Promise((resolve, reject) => {
    if (!process.stdin.isTTY) return reject(new Error("The Charm Repo is protected: set CHARM_REPO_PASSWORD (Paul's login password) or run this in a terminal to be asked for it"));
    const rl = require("readline").createInterface({ input: process.stdin, output: process.stderr, terminal: true });
    rl._writeToOutput = s => { if (!rl.muted) rl.output.write(s); };
    rl.question(q, a => { rl.close(); process.stderr.write("\n"); resolve(a); });
    rl.muted = true;
  });
}
async function unlock(origin, passcode, action, who) {
  if (!password) password = await ask(`Charm Repo password for ${action} (typed here, hidden): `);
  const headers = { "Content-Type": "application/json" }; if (passcode) headers["X-Edit-Passcode"] = passcode;
  const res = await fetch(`${origin}/.netlify/functions/charmNestLibrary`, { method: "POST", headers, body: JSON.stringify({ op: "repoUnlock", password, action, by: "script", target: String(who || "script").slice(0, 60) }) });
  const data = await res.json().catch(() => ({}));
  if (!data.ok) { if (data.error === "wrong") password = ""; throw new Error(data.error === "wrong" ? "The Charm Repo password was not accepted" : data.error === "locked" ? `The Charm Repo is locked for ${data.retryAfterSec || "a few"} seconds after wrong tries` : `The Charm Repo could not be unlocked (${data.error || "HTTP " + res.status})`); }
  return { token: data.token, exp: Date.now() + RepoAuth.TTL_MS[action] };   // (this machine's clock: the site's expiresAt is on the site's)
}
/** The body, with a repoToken when the call needs one (a free call comes back as it was). */
async function stamp(origin, passcode, fn, body) {
  if (!body || body.repoToken) return body;
  const need = RepoAuth.requirement(fn, body.op, body); if (!need) return body;
  const del = need.need.startsWith("delete"), action = del ? "delete" : need.need === "download" ? "download" : "upload";
  let h = held.get(action);
  if (del || !h || h.exp - Date.now() < 30000) { h = await unlock(origin, passcode, action, process.argv[1] ? require("path").basename(process.argv[1]) : "script"); if (!del) held.set(action, h); }
  return Object.assign({}, body, { repoToken: h.token });
}
module.exports = { stamp };
