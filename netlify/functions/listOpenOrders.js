/********************************************************
 * buildNewOrderList – always returns up to 100 displayable
 * receipts. Pulls extra Etsy pages as needed so each UI
 * “page” is full, except the very last one.
 ********************************************************/
async function buildNewOrderList() {
  /* 1) must have valid token */
  let token = localStorage.getItem("access_token") || "";
  if (!token) {
    M.toast({ html: "Connect to Etsy first!" });
    return false;
  }

  /* 2) start with leftovers from the previous fetch */
  let displayReceipts = [...carryReceipts];
  carryReceipts = [];   // we’ll refill this later
  let offset = nextOffsetCursor;

  /* 3) fetch additional Etsy pages until we hit 100 or Etsy runs dry */
  while (displayReceipts.length < 100 && offset !== null) {
    let resp = await fetch(
      `${functionsBaseUrl}/listOpenOrders?offset=${offset}`,
      { headers: { "access-token": token } }
    );

    /* token expired? – refresh once then retry */
    if (resp.status === 401) {
      const ok = await refreshAccessToken();
      if (!ok) throw new Error("HTTP 401 — token refresh failed");
      token = localStorage.getItem("access_token") || "";
      resp = await fetch(
        `${functionsBaseUrl}/listOpenOrders?offset=${offset}`,
        { headers: { "access-token": token } }
      );
    }

    if (!resp.ok) throw new Error("HTTP " + resp.status);

    const payload   = await resp.json();
    const receipts  = (payload.results || []).filter(
      r => r.status === "Paid" && r.is_shipped === false
    );

    displayReceipts.push(...receipts);
    offset = (payload.pagination || {}).next_offset ?? null;   // null when done
  }

  /* 4) split into:  first 100 → render  |  rest → carryReceipts */
  const pageReceipts   = displayReceipts.slice(0, 100);
  carryReceipts        = displayReceipts.slice(100);   // may be empty
  nextOffsetCursor     = offset;                       // where Etsy left off

  /* 5) draw the UI */
  const container = document.getElementById("newOrderContainer");
  container.innerHTML = "";

  pageReceipts.forEach((r, idx) => {
    const orderNum  = r.order_number || r.receipt_id || "—";
    let shipStr     = "N/A";
    let ts          = null;

    if (Array.isArray(r.transactions) && r.transactions.length) {
      const firstT = r.transactions[0];
      if (firstT.expected_ship_date) ts = firstT.expected_ship_date;
    }
    if (!ts) ts = r.dispatch_date || r.ship_by_date;
    if (ts) {
      const d  = new Date(ts * 1000);
      const dd = ("0" + d.getDate()).slice(-2);
      const mm = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getMonth()];
      shipStr  = `${dd} ${mm} ${d.getFullYear()}`;
    }

    const totalQty = (r.transactions || []).reduce(
      (sum, t) => sum + (Number(t.quantity) || 0), 0
    );

    const box = document.createElement("div");
    box.className = "new-order-box";
    box.id        = "newOrder" + idx;
    box.innerHTML = `
      ${orderNum}&nbsp;|&nbsp;${shipStr}&nbsp;|&nbsp;Qty&nbsp;${totalQty}
      <input type="checkbox" class="addToDesignChk" title="Add to Design">
    `;
    container.appendChild(box);
  });

  /* 6) tell the buttons whether there’s more work */
  const stillMore = carryReceipts.length > 0 || nextOffsetCursor !== null;
  return stillMore;
}