// netlify/functions/trackOrderDiagnostic.js
// Diagnostic-only ShipStation probe for Etsy order completion failures.
// Does NOT modify the existing trackOrderProxy.js.
//
// ENV VARS:
//   SS_API_KEY
//   SS_API_SECRET
//
// Modes:
//   inspect (default): read-only; finds the ShipStation order and connected store.
//   replay: performs the same mark-as-shipped call as production. This changes the
//           ShipStation order state, so use only on an order you genuinely intend to complete.

const SS_BASE = "https://ssapi.shipstation.com";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
};

function json(statusCode, payload) {
  return {
    statusCode,
    headers: corsHeaders,
    body: JSON.stringify(payload, null, 2),
  };
}

function safeJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text || null;
  }
}

function headerSubset(headers) {
  const wanted = [
    "content-type",
    "date",
    "server",
    "x-rate-limit-limit",
    "x-rate-limit-remaining",
    "x-rate-limit-reset",
    "x-request-id",
    "request-id",
    "cf-ray",
  ];
  const out = {};
  for (const name of wanted) {
    const value = headers.get(name);
    if (value != null) out[name] = value;
  }
  return out;
}

async function ssFetch(path, options, headers) {
  const startedAt = Date.now();
  const response = await fetch(`${SS_BASE}${path}`, {
    ...options,
    headers: {
      ...headers,
      ...(options?.headers || {}),
    },
  });
  const text = await response.text();
  return {
    ok: response.ok,
    status: response.status,
    statusText: response.statusText,
    elapsedMs: Date.now() - startedAt,
    headers: headerSubset(response.headers),
    body: safeJson(text),
  };
}

function normalizeCarrier(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "chitchats" || raw === "chit_chats" || raw === "chit chats") return "other";
  return raw;
}

function summarizeOrder(order) {
  if (!order || typeof order !== "object") return null;
  return {
    orderId: order.orderId ?? null,
    orderKey: order.orderKey ?? null,
    orderNumber: order.orderNumber ?? null,
    orderStatus: order.orderStatus ?? null,
    createDate: order.createDate ?? null,
    modifyDate: order.modifyDate ?? null,
    paymentDate: order.paymentDate ?? null,
    shipByDate: order.shipByDate ?? null,
    customerUsername: order.customerUsername ?? null,
    externallyFulfilled: order.externallyFulfilled ?? null,
    externallyFulfilledBy: order.externallyFulfilledBy ?? null,
    carrierCode: order.carrierCode ?? null,
    serviceCode: order.serviceCode ?? null,
    advancedOptions: {
      storeId: order.advancedOptions?.storeId ?? null,
      source: order.advancedOptions?.source ?? null,
      customField1: order.advancedOptions?.customField1 ?? null,
      customField2: order.advancedOptions?.customField2 ?? null,
      customField3: order.advancedOptions?.customField3 ?? null,
    },
  };
}

function summarizeStore(store) {
  if (!store || typeof store !== "object") return null;
  return {
    storeId: store.storeId ?? null,
    storeName: store.storeName ?? null,
    marketplaceName: store.marketplaceName ?? null,
    marketplaceId: store.marketplaceId ?? null,
    accountName: store.accountName ?? null,
    active: store.active ?? null,
    refreshDate: store.refreshDate ?? null,
    lastRefreshAttempt: store.lastRefreshAttempt ?? null,
    autoRefresh: store.autoRefresh ?? null,
    statusMappings: store.statusMappings ?? null,
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: corsHeaders, body: "" };
  }

  const { SS_API_KEY = "", SS_API_SECRET = "" } = process.env;
  const envCheck = {
    SS_API_KEY: Boolean(SS_API_KEY),
    SS_API_SECRET: Boolean(SS_API_SECRET),
  };

  if (event.httpMethod === "GET") {
    return json(200, {
      ok: true,
      function: "trackOrderDiagnostic",
      defaultMode: "inspect",
      envCheck,
      warning: "replay mode performs a real ShipStation mark-as-shipped operation",
    });
  }

  if (event.httpMethod !== "POST") {
    return json(405, { ok: false, error: "Method Not Allowed" });
  }

  if (!SS_API_KEY || !SS_API_SECRET) {
    return json(500, {
      ok: false,
      stage: "environment",
      error: "Missing SS_API_KEY or SS_API_SECRET",
      envCheck,
    });
  }

  let input;
  try {
    input = JSON.parse(event.body || "{}");
  } catch {
    return json(400, { ok: false, error: "Invalid JSON body" });
  }

  const mode = String(input.mode || "inspect").trim().toLowerCase();
  const orderNumber = String(input.orderNumber || input.receiptId || "").trim();
  const trackingNumber = String(input.trackingNumber || input.tracking || "").trim();
  const carrierCode = normalizeCarrier(input.carrierCode || input.carrier);
  const shipDate = String(input.shipDate || new Date().toISOString().slice(0, 10)).trim();

  if (!orderNumber) {
    return json(400, { ok: false, error: "orderNumber is required" });
  }

  const auth = Buffer.from(`${SS_API_KEY}:${SS_API_SECRET}`).toString("base64");
  const ssHeaders = {
    Authorization: `Basic ${auth}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  const diagnostic = {
    ok: false,
    mode,
    timestamp: new Date().toISOString(),
    input: {
      orderNumber,
      carrierInput: input.carrierCode || input.carrier || null,
      normalizedCarrierCode: carrierCode || null,
      shipDate,
      trackingProvided: Boolean(trackingNumber),
      trackingLast4: trackingNumber ? trackingNumber.slice(-4) : null,
    },
    stages: {},
  };

  try {
    // Stage 1: prove ShipStation API credentials and locate the order.
    const orderSearch = await ssFetch(
      `/orders?orderNumber=${encodeURIComponent(orderNumber)}`,
      { method: "GET" },
      ssHeaders
    );
    diagnostic.stages.orderSearch = orderSearch;

    if (!orderSearch.ok) {
      diagnostic.failureStage = "orderSearch";
      diagnostic.conclusion =
        orderSearch.status === 401
          ? "ShipStation API credentials were rejected."
          : orderSearch.status === 403
            ? "ShipStation rejected the read request before any Etsy notification was attempted."
            : "ShipStation order lookup failed.";
      return json(200, diagnostic);
    }

    const orders = Array.isArray(orderSearch.body?.orders) ? orderSearch.body.orders : [];
    const exact = orders.find((o) => String(o.orderNumber) === orderNumber);
    const order = exact || orders[0] || null;
    diagnostic.order = summarizeOrder(order);
    diagnostic.orderSearchMatchCount = orders.length;
    diagnostic.orderSearchExactMatch = Boolean(exact);

    if (!order?.orderId) {
      diagnostic.failureStage = "orderResolution";
      diagnostic.conclusion = "ShipStation API works, but no matching order was found.";
      return json(200, diagnostic);
    }

    // Stage 2: read the exact order by ID.
    const orderDetail = await ssFetch(`/orders/${encodeURIComponent(order.orderId)}`, { method: "GET" }, ssHeaders);
    diagnostic.stages.orderDetail = orderDetail;
    if (orderDetail.ok) diagnostic.order = summarizeOrder(orderDetail.body);

    // Stage 3: inspect the connected selling-channel store associated with this order.
    const storeId = orderDetail.body?.advancedOptions?.storeId ?? order.advancedOptions?.storeId;
    diagnostic.storeId = storeId ?? null;

    if (storeId != null) {
      const storeDetail = await ssFetch(`/stores/${encodeURIComponent(storeId)}`, { method: "GET" }, ssHeaders);
      diagnostic.stages.storeDetail = storeDetail;
      if (storeDetail.ok) diagnostic.store = summarizeStore(storeDetail.body);
    } else {
      diagnostic.stages.storeDetail = {
        skipped: true,
        reason: "The ShipStation order did not contain advancedOptions.storeId",
      };
    }

    if (mode === "inspect") {
      diagnostic.ok = true;
      diagnostic.conclusion =
        "Read-only inspection completed. ShipStation credentials, order lookup, order state, and associated store details are shown above. No order was modified.";
      return json(200, diagnostic);
    }

    if (mode !== "replay") {
      diagnostic.failureStage = "input";
      diagnostic.conclusion = "Unknown mode. Use inspect or replay.";
      return json(400, diagnostic);
    }

    if (!trackingNumber || !carrierCode) {
      diagnostic.failureStage = "input";
      diagnostic.conclusion = "Replay requires trackingNumber and carrierCode.";
      return json(400, diagnostic);
    }

    if (input.confirmReplay !== "MARK_AS_SHIPPED") {
      diagnostic.failureStage = "safetyCheck";
      diagnostic.conclusion =
        "Replay was blocked. Set confirmReplay to MARK_AS_SHIPPED to acknowledge that this performs a real completion operation.";
      return json(200, diagnostic);
    }

    const notifySalesChannel = input.notifySalesChannel !== false;
    const notifyCustomer = input.notifyCustomer === true;
    const markBody = {
      orderId: order.orderId,
      carrierCode,
      shipDate,
      trackingNumber,
      notifyCustomer,
      notifySalesChannel,
    };

    diagnostic.replayRequest = {
      ...markBody,
      trackingNumber: `***${trackingNumber.slice(-4)}`,
    };

    const markAsShipped = await ssFetch(
      "/orders/markasshipped",
      { method: "POST", body: JSON.stringify(markBody) },
      ssHeaders
    );
    diagnostic.stages.markAsShipped = markAsShipped;

    diagnostic.ok = markAsShipped.ok;
    diagnostic.failureStage = markAsShipped.ok ? null : "markAsShipped";

    if (markAsShipped.ok) {
      diagnostic.conclusion = notifySalesChannel
        ? "ShipStation accepted the completion and marketplace-notification request."
        : "ShipStation accepted the completion request with marketplace notification disabled.";
    } else if (markAsShipped.status === 401) {
      diagnostic.conclusion = "ShipStation rejected the API credentials during mark-as-shipped.";
    } else if (markAsShipped.status === 403) {
      diagnostic.conclusion =
        "ShipStation accepted the read-only API calls but returned 403 specifically for mark-as-shipped. The raw ShipStation body and request headers above should identify whether this is order state, account permission, carrier, or selling-channel notification authorization.";
    } else {
      diagnostic.conclusion = "ShipStation mark-as-shipped failed; inspect stages.markAsShipped.body for the exact upstream reason.";
    }

    return json(200, diagnostic);
  } catch (error) {
    diagnostic.failureStage = "unexpectedException";
    diagnostic.exception = {
      name: error?.name || "Error",
      message: error?.message || String(error),
      stack: error?.stack || null,
    };
    diagnostic.conclusion = "The diagnostic function itself threw before completing all stages.";
    return json(200, diagnostic);
  }
};
