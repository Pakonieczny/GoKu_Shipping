/*  netlify/functions/_charmNestSolver.js
 *  The browser and the server run the SAME solver source. This module only
 *  forwards to the repo-root file so esbuild bundles it into
 *  charmNestSolve-background; there is no second copy to drift.            */
"use strict";
module.exports = require("../../charm-nest-solver.js");
