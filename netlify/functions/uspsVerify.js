/* /.netlify/functions/uspsVerify.js
 * USPS Web Tools — Address Validate (US-only)
 * Env: USPS_USERID
 */
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "OPTIONS,POST"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const USERID = process.env.USPS_USERID;
    if (!USERID) {
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: "Missing USPS_USERID" }) };
    }

    const body = JSON.parse(event.body || "{}");
    const a = {
      address1: (body.address1 || "").trim(),  // primary line
      address2: (body.address2 || "").trim(),  // secondary (apt, unit)
      city    : (body.city || "").trim(),
      state   : (body.state || "").trim(),
      postal  : (body.postal || "").trim()
    };

    const xml = `
      <AddressValidateRequest USERID="${USERID}">
        <Revision>1</Revision>
        <Address ID="0">
          <Address1>${escapeXml(a.address2)}</Address1>
          <Address2>${escapeXml(a.address1)}</Address2>
          <City>${escapeXml(a.city)}</City>
          <State>${escapeXml(a.state)}</State>
          <Zip5>${escapeXml(a.postal.slice(0,5))}</Zip5>
          <Zip4></Zip4>
        </Address>
      </AddressValidateRequest>`.trim();

    const url = "https://secure.shippingapis.com/ShippingAPI.dll?API=Verify&XML=" + encodeURIComponent(xml);
    const resp = await fetch(url);
    const text = await resp.text();

    // Gracefully parse the tiny USPS XML without deps
    if (/<Error>/i.test(text)) {
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested: null, raw: text }) };
    }

    const pick = (tag) => (text.match(new RegExp(`<${tag}>([^<]*)</${tag}>`, "i")) || [,""])[1].trim();
    const Address1 = pick("Address1"); // secondary (apt)
    const Address2 = pick("Address2"); // primary
    const City     = pick("City");
    const State    = pick("State");
    const Zip5     = pick("Zip5");
    const Zip4     = pick("Zip4");
    const postal   = Zip4 ? `${Zip5}-${Zip4}` : Zip5;

    const suggested = {
      address1: Address2 || "", // normalized primary
      address2: Address1 || "",
      city    : City,
      state   : State,
      postal  : postal,
      country : "US"
    };

    return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested, raw: text }) };
  } catch (err) {
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested: null, error: err.message }) };
  }
};

function escapeXml(s = "") {
  return String(s).replace(/[<>&'"]/g, c => ({ "<":"&lt;", ">":"&gt;", "&":"&amp;", "'":"&apos;", '"':"&quot;" }[c]));
}