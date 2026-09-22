'use strict';
// Both public image routes share the same durable cache, lock and budget.
exports.handler=require('./etsyImages').handler;
