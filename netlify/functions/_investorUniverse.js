/*  netlify/functions/_investorUniverse.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — frozen universe configuration.
 *
 *  This is a .js module rather than a .json data file on purpose. Two builds
 *  failed because the config lived somewhere the bundler could not reach: once
 *  in a sibling investor/ folder that never made it into the repo, and once as
 *  a .json that was not copied alongside the code. A .js file in this folder
 *  travels with every other _investor* helper and esbuild resolves it with
 *  certainty, so that class of failure is gone.
 *
 *  EDIT THIS FILE to retune. It is read at runtime by investorCycle-background
 *  and investorApi. To run a different version, copy to _investorStrategy.v2.js,
 *  register it in Firestore, and switch versions from the control room — never
 *  edit an active version in place.
 * ---------------------------------------------------------------------------
 */

"use strict";

// Exact frozen SEC identity snapshot, embedded so Netlify has no external JSON asset to resolve.
const IDENTITY = Object.freeze({"schema":"sec-company-identity-snapshot-v1","universeVersion":"v6","source":{"url":"https://www.sec.gov/files/company_tickers.json","retrievedDate":"2026-09-01","responseSha256":"bf83a6d3c92cfd32211e60521861d7b0fb0ae8cf321284d4e66d01ef295ec051"},"count":304,"companies":{"NVDA":{"cik":"1045810","company":"NVIDIA CORP","secTicker":"NVDA"},"AMD":{"cik":"2488","company":"ADVANCED MICRO DEVICES INC","secTicker":"AMD"},"MU":{"cik":"723125","company":"MICRON TECHNOLOGY INC","secTicker":"MU"},"INTC":{"cik":"50863","company":"INTEL CORP","secTicker":"INTC"},"AVGO":{"cik":"1730168","company":"Broadcom Inc.","secTicker":"AVGO"},"QCOM":{"cik":"804328","company":"QUALCOMM INC/DE","secTicker":"QCOM"},"TXN":{"cik":"97476","company":"TEXAS INSTRUMENTS INC","secTicker":"TXN"},"MRVL":{"cik":"1835632","company":"Marvell Technology, Inc.","secTicker":"MRVL"},"ADI":{"cik":"6281","company":"ANALOG DEVICES INC","secTicker":"ADI"},"NXPI":{"cik":"1413447","company":"NXP Semiconductors N.V.","secTicker":"NXPI"},"MCHP":{"cik":"827054","company":"MICROCHIP TECHNOLOGY INC","secTicker":"MCHP"},"ON":{"cik":"1097864","company":"ON SEMICONDUCTOR CORP","secTicker":"ON"},"SWKS":{"cik":"4127","company":"SKYWORKS SOLUTIONS, INC.","secTicker":"SWKS"},"QRVO":{"cik":"1604778","company":"Qorvo, Inc.","secTicker":"QRVO"},"MPWR":{"cik":"1280452","company":"MONOLITHIC POWER SYSTEMS, INC.","secTicker":"MPWR"},"LSCC":{"cik":"855658","company":"LATTICE SEMICONDUCTOR CORP","secTicker":"LSCC"},"ALAB":{"cik":"1736297","company":"Astera Labs, Inc.","secTicker":"ALAB"},"CRDO":{"cik":"1807794","company":"Credo Technology Group Holding Ltd","secTicker":"CRDO"},"AMKR":{"cik":"1047127","company":"AMKOR TECHNOLOGY, INC.","secTicker":"AMKR"},"ONTO":{"cik":"704532","company":"ONTO INNOVATION INC.","secTicker":"ONTO"},"ENTG":{"cik":"1101302","company":"ENTEGRIS INC","secTicker":"ENTG"},"MKSI":{"cik":"1049502","company":"MKS INC","secTicker":"MKSI"},"ACLS":{"cik":"1113232","company":"AXCELIS TECHNOLOGIES INC","secTicker":"ACLS"},"UCTT":{"cik":"1275014","company":"Ultra Clean Holdings, Inc.","secTicker":"UCTT"},"ICHR":{"cik":"1652535","company":"ICHOR HOLDINGS, LTD.","secTicker":"ICHR"},"AMAT":{"cik":"6951","company":"APPLIED MATERIALS INC /DE","secTicker":"AMAT"},"LRCX":{"cik":"707549","company":"LAM RESEARCH CORP","secTicker":"LRCX"},"KLAC":{"cik":"319201","company":"KLA CORP","secTicker":"KLAC"},"TER":{"cik":"97210","company":"TERADYNE, INC","secTicker":"TER"},"COHR":{"cik":"820318","company":"COHERENT CORP.","secTicker":"COHR"},"WOLF":{"cik":"895419","company":"WOLFSPEED, INC.","secTicker":"WOLF"},"SLAB":{"cik":"1038074","company":"SILICON LABORATORIES INC.","secTicker":"SLAB"},"SITM":{"cik":"1451809","company":"SITIME Corp","secTicker":"SITM"},"POWI":{"cik":"833640","company":"POWER INTEGRATIONS INC","secTicker":"POWI"},"DIOD":{"cik":"29002","company":"DIODES INC /DEL/","secTicker":"DIOD"},"DELL":{"cik":"1571996","company":"Dell Technologies Inc.","secTicker":"DELL"},"HPQ":{"cik":"47217","company":"HP INC","secTicker":"HPQ"},"HPE":{"cik":"1645590","company":"Hewlett Packard Enterprise Co","secTicker":"HPE"},"SMCI":{"cik":"1375365","company":"Super Micro Computer, Inc.","secTicker":"SMCI"},"WDC":{"cik":"106040","company":"WESTERN DIGITAL CORP","secTicker":"WDC"},"STX":{"cik":"1137789","company":"Seagate Technology Holdings plc","secTicker":"STX"},"NTAP":{"cik":"1002047","company":"NetApp, Inc.","secTicker":"NTAP"},"FFIV":{"cik":"1048695","company":"F5, INC.","secTicker":"FFIV"},"ANET":{"cik":"1596532","company":"Arista Networks, Inc.","secTicker":"ANET"},"CSCO":{"cik":"858877","company":"CISCO SYSTEMS, INC.","secTicker":"CSCO"},"LITE":{"cik":"1633978","company":"Lumentum Holdings Inc.","secTicker":"LITE"},"VRT":{"cik":"1674101","company":"Vertiv Holdings Co","secTicker":"VRT"},"NVT":{"cik":"1720635","company":"nVent Electric plc","secTicker":"NVT"},"GEV":{"cik":"1996810","company":"GE Vernova Inc.","secTicker":"GEV"},"ZBRA":{"cik":"877212","company":"ZEBRA TECHNOLOGIES CORP","secTicker":"ZBRA"},"KEYS":{"cik":"1601046","company":"Keysight Technologies, Inc.","secTicker":"KEYS"},"TDY":{"cik":"1094285","company":"TELEDYNE TECHNOLOGIES INC","secTicker":"TDY"},"TRMB":{"cik":"864749","company":"TRIMBLE INC.","secTicker":"TRMB"},"FLEX":{"cik":"866374","company":"FLEX LTD.","secTicker":"FLEX"},"JBL":{"cik":"898293","company":"JABIL INC","secTicker":"JBL"},"SANM":{"cik":"897723","company":"SANMINA CORP","secTicker":"SANM"},"BHE":{"cik":"863436","company":"BENCHMARK ELECTRONICS INC","secTicker":"BHE"},"PLXS":{"cik":"785786","company":"PLEXUS CORP","secTicker":"PLXS"},"FN":{"cik":"1408710","company":"Fabrinet","secTicker":"FN"},"MSFT":{"cik":"789019","company":"MICROSOFT CORP","secTicker":"MSFT"},"ORCL":{"cik":"1341439","company":"ORACLE CORP","secTicker":"ORCL"},"CRM":{"cik":"1108524","company":"Salesforce, Inc.","secTicker":"CRM"},"ADBE":{"cik":"796343","company":"ADOBE INC.","secTicker":"ADBE"},"INTU":{"cik":"896878","company":"INTUIT INC.","secTicker":"INTU"},"NOW":{"cik":"1373715","company":"ServiceNow, Inc.","secTicker":"NOW"},"WDAY":{"cik":"1327811","company":"Workday, Inc.","secTicker":"WDAY"},"SNOW":{"cik":"1640147","company":"Snowflake Inc.","secTicker":"SNOW"},"PANW":{"cik":"1327567","company":"Palo Alto Networks Inc","secTicker":"PANW"},"CRWD":{"cik":"1535527","company":"CrowdStrike Holdings, Inc.","secTicker":"CRWD"},"ZS":{"cik":"1713683","company":"Zscaler, Inc.","secTicker":"ZS"},"OKTA":{"cik":"1660134","company":"Okta, Inc.","secTicker":"OKTA"},"NET":{"cik":"1477333","company":"Cloudflare, Inc.","secTicker":"NET"},"DDOG":{"cik":"1561550","company":"Datadog, Inc.","secTicker":"DDOG"},"MDB":{"cik":"1441816","company":"MongoDB, Inc.","secTicker":"MDB"},"TEAM":{"cik":"1650372","company":"Atlassian Corp","secTicker":"TEAM"},"HUBS":{"cik":"1404655","company":"HUBSPOT INC","secTicker":"HUBS"},"VEEV":{"cik":"1393052","company":"VEEVA SYSTEMS INC","secTicker":"VEEV"},"TYL":{"cik":"860731","company":"TYLER TECHNOLOGIES INC","secTicker":"TYL"},"PTC":{"cik":"857005","company":"PTC INC.","secTicker":"PTC"},"BSY":{"cik":"1031308","company":"BENTLEY SYSTEMS INC","secTicker":"BSY"},"CDNS":{"cik":"813672","company":"CADENCE DESIGN SYSTEMS INC","secTicker":"CDNS"},"SNPS":{"cik":"883241","company":"SYNOPSYS INC","secTicker":"SNPS"},"ADSK":{"cik":"769397","company":"Autodesk, Inc.","secTicker":"ADSK"},"ROP":{"cik":"882835","company":"ROPER TECHNOLOGIES INC","secTicker":"ROP"},"FTNT":{"cik":"1262039","company":"Fortinet, Inc.","secTicker":"FTNT"},"GEN":{"cik":"849399","company":"Gen Digital Inc.","secTicker":"GEN"},"DOCU":{"cik":"1261333","company":"DOCUSIGN, INC.","secTicker":"DOCU"},"ZM":{"cik":"1585521","company":"Zoom Communications, Inc.","secTicker":"ZM"},"TWLO":{"cik":"1447669","company":"TWILIO INC","secTicker":"TWLO"},"BILL":{"cik":"1786352","company":"BILL Holdings, Inc.","secTicker":"BILL"},"PCTY":{"cik":"1591698","company":"Paylocity Holding Corp","secTicker":"PCTY"},"PAYC":{"cik":"1590955","company":"Paycom Software, Inc.","secTicker":"PAYC"},"AMZN":{"cik":"1018724","company":"AMAZON COM INC","secTicker":"AMZN"},"GOOGL":{"cik":"1652044","company":"Alphabet Inc.","secTicker":"GOOGL"},"META":{"cik":"1326801","company":"Meta Platforms, Inc.","secTicker":"META"},"NFLX":{"cik":"1065280","company":"NETFLIX INC","secTicker":"NFLX"},"DIS":{"cik":"1744489","company":"Walt Disney Co","secTicker":"DIS"},"SPOT":{"cik":"1639920","company":"Spotify Technology S.A.","secTicker":"SPOT"},"RBLX":{"cik":"1315098","company":"Roblox Corp","secTicker":"RBLX"},"TTWO":{"cik":"946581","company":"TAKE TWO INTERACTIVE SOFTWARE INC","secTicker":"TTWO"},"APP":{"cik":"1751008","company":"AppLovin Corp","secTicker":"APP"},"TTD":{"cik":"1671933","company":"Trade Desk, Inc.","secTicker":"TTD"},"PINS":{"cik":"1506293","company":"PINTEREST, INC.","secTicker":"PINS"},"SNAP":{"cik":"1564408","company":"Snap Inc","secTicker":"SNAP"},"RDDT":{"cik":"1713445","company":"Reddit, Inc.","secTicker":"RDDT"},"UBER":{"cik":"1543151","company":"Uber Technologies, Inc","secTicker":"UBER"},"LYFT":{"cik":"1759509","company":"Lyft, Inc.","secTicker":"LYFT"},"DASH":{"cik":"1792789","company":"DoorDash, Inc.","secTicker":"DASH"},"ABNB":{"cik":"1559720","company":"Airbnb, Inc.","secTicker":"ABNB"},"BKNG":{"cik":"1075531","company":"Booking Holdings Inc.","secTicker":"BKNG"},"EXPE":{"cik":"1324424","company":"Expedia Group, Inc.","secTicker":"EXPE"},"ETSY":{"cik":"1370637","company":"ETSY INC","secTicker":"ETSY"},"EBAY":{"cik":"1065088","company":"EBAY INC","secTicker":"EBAY"},"CHWY":{"cik":"1766502","company":"Chewy, Inc.","secTicker":"CHWY"},"W":{"cik":"1616707","company":"Wayfair Inc.","secTicker":"W"},"SHOP":{"cik":"1594805","company":"SHOPIFY INC.","secTicker":"SHOP"},"WFC":{"cik":"72971","company":"WELLS FARGO & COMPANY/MN","secTicker":"WFC"},"C":{"cik":"831001","company":"CITIGROUP INC","secTicker":"C"},"BX":{"cik":"1393818","company":"Blackstone Inc.","secTicker":"BX"},"KKR":{"cik":"1404912","company":"KKR & Co. Inc.","secTicker":"KKR"},"APO":{"cik":"1858681","company":"Apollo Global Management, Inc.","secTicker":"APO"},"ARES":{"cik":"1176948","company":"Ares Management Corp","secTicker":"ARES"},"COIN":{"cik":"1679788","company":"Coinbase Global, Inc.","secTicker":"COIN"},"HOOD":{"cik":"1783879","company":"Robinhood Markets, Inc.","secTicker":"HOOD"},"SOFI":{"cik":"1818874","company":"SoFi Technologies, Inc.","secTicker":"SOFI"},"PYPL":{"cik":"1633917","company":"PayPal Holdings, Inc.","secTicker":"PYPL"},"V":{"cik":"1403161","company":"VISA INC.","secTicker":"V"},"MA":{"cik":"1141391","company":"Mastercard Inc","secTicker":"MA"},"AXP":{"cik":"4962","company":"AMERICAN EXPRESS CO","secTicker":"AXP"},"COF":{"cik":"927628","company":"CAPITAL ONE FINANCIAL CORP","secTicker":"COF"},"IBKR":{"cik":"1381197","company":"Interactive Brokers Group, Inc.","secTicker":"IBKR"},"SYF":{"cik":"1601712","company":"Synchrony Financial","secTicker":"SYF"},"FIS":{"cik":"1136893","company":"Fidelity National Information Services, Inc.","secTicker":"FIS"},"GPN":{"cik":"1123360","company":"GLOBAL PAYMENTS INC","secTicker":"GPN"},"ICE":{"cik":"1571949","company":"Intercontinental Exchange, Inc.","secTicker":"ICE"},"CME":{"cik":"1156375","company":"CME GROUP INC.","secTicker":"CME"},"NDAQ":{"cik":"1120193","company":"NASDAQ, INC.","secTicker":"NDAQ"},"SPGI":{"cik":"64040","company":"S&P Global Inc.","secTicker":"SPGI"},"MCO":{"cik":"1059556","company":"MOODYS CORP /DE/","secTicker":"MCO"},"MSCI":{"cik":"1408198","company":"MSCI Inc.","secTicker":"MSCI"},"TROW":{"cik":"1113169","company":"PRICE T ROWE GROUP INC","secTicker":"TROW"},"BEN":{"cik":"38777","company":"FRANKLIN RESOURCES INC","secTicker":"BEN"},"IVZ":{"cik":"914208","company":"Invesco Ltd.","secTicker":"IVZ"},"STT":{"cik":"93751","company":"STATE STREET CORP","secTicker":"STT"},"NTRS":{"cik":"73124","company":"NORTHERN TRUST CORP","secTicker":"NTRS"},"ELV":{"cik":"1156039","company":"Elevance Health, Inc.","secTicker":"ELV"},"HUM":{"cik":"49071","company":"HUMANA INC","secTicker":"HUM"},"CNC":{"cik":"1071739","company":"CENTENE CORP","secTicker":"CNC"},"MCK":{"cik":"927653","company":"MCKESSON CORP","secTicker":"MCK"},"COR":{"cik":"1140859","company":"Cencora, Inc.","secTicker":"COR"},"CAH":{"cik":"721371","company":"CARDINAL HEALTH INC","secTicker":"CAH"},"ABT":{"cik":"1800","company":"ABBOTT LABORATORIES","secTicker":"ABT"},"BSX":{"cik":"885725","company":"BOSTON SCIENTIFIC CORP","secTicker":"BSX"},"SYK":{"cik":"310764","company":"STRYKER CORP","secTicker":"SYK"},"MDT":{"cik":"1613103","company":"Medtronic plc","secTicker":"MDT"},"ISRG":{"cik":"1035267","company":"INTUITIVE SURGICAL INC","secTicker":"ISRG"},"EW":{"cik":"1099800","company":"Edwards Lifesciences Corp","secTicker":"EW"},"DXCM":{"cik":"1093557","company":"DEXCOM INC","secTicker":"DXCM"},"PODD":{"cik":"1145197","company":"INSULET CORP","secTicker":"PODD"},"ZBH":{"cik":"1136869","company":"ZIMMER BIOMET HOLDINGS, INC.","secTicker":"ZBH"},"BDX":{"cik":"10795","company":"BECTON DICKINSON & CO","secTicker":"BDX"},"BAX":{"cik":"10456","company":"BAXTER INTERNATIONAL INC","secTicker":"BAX"},"RMD":{"cik":"943819","company":"RESMED INC","secTicker":"RMD"},"ALGN":{"cik":"1097149","company":"ALIGN TECHNOLOGY INC","secTicker":"ALGN"},"TMO":{"cik":"97745","company":"THERMO FISHER SCIENTIFIC INC.","secTicker":"TMO"},"DHR":{"cik":"313616","company":"DANAHER CORP /DE/","secTicker":"DHR"},"A":{"cik":"1090872","company":"AGILENT TECHNOLOGIES, INC.","secTicker":"A"},"WAT":{"cik":"1000697","company":"WATERS CORP /DE/","secTicker":"WAT"},"MTD":{"cik":"1037646","company":"METTLER TOLEDO INTERNATIONAL INC/","secTicker":"MTD"},"IQV":{"cik":"1478242","company":"IQVIA HOLDINGS INC.","secTicker":"IQV"},"CRL":{"cik":"1100682","company":"CHARLES RIVER LABORATORIES INTERNATIONAL, INC.","secTicker":"CRL"},"HCA":{"cik":"860730","company":"HCA Healthcare, Inc.","secTicker":"HCA"},"UHS":{"cik":"352915","company":"UNIVERSAL HEALTH SERVICES INC","secTicker":"UHS"},"THC":{"cik":"70318","company":"TENET HEALTHCARE CORP","secTicker":"THC"},"DVA":{"cik":"927066","company":"DAVITA INC.","secTicker":"DVA"},"LLY":{"cik":"59478","company":"ELI LILLY & Co","secTicker":"LLY"},"MRK":{"cik":"310158","company":"Merck & Co., Inc.","secTicker":"MRK"},"PFE":{"cik":"78003","company":"PFIZER INC","secTicker":"PFE"},"ABBV":{"cik":"1551152","company":"AbbVie Inc.","secTicker":"ABBV"},"BMY":{"cik":"14272","company":"BRISTOL MYERS SQUIBB CO","secTicker":"BMY"},"AMGN":{"cik":"318154","company":"AMGEN INC","secTicker":"AMGN"},"GILD":{"cik":"882095","company":"GILEAD SCIENCES, INC.","secTicker":"GILD"},"VRTX":{"cik":"875320","company":"VERTEX PHARMACEUTICALS INC / MA","secTicker":"VRTX"},"REGN":{"cik":"872589","company":"REGENERON PHARMACEUTICALS, INC.","secTicker":"REGN"},"CAT":{"cik":"18230","company":"CATERPILLAR INC","secTicker":"CAT"},"DE":{"cik":"315189","company":"DEERE & CO","secTicker":"DE"},"HON":{"cik":"773840","company":"HONEYWELL INTERNATIONAL INC","secTicker":"HON"},"GE":{"cik":"40545","company":"GENERAL ELECTRIC CO","secTicker":"GE"},"RTX":{"cik":"101829","company":"RTX Corp","secTicker":"RTX"},"LMT":{"cik":"936468","company":"LOCKHEED MARTIN CORP","secTicker":"LMT"},"NOC":{"cik":"1133421","company":"NORTHROP GRUMMAN CORP /DE/","secTicker":"NOC"},"GD":{"cik":"40533","company":"GENERAL DYNAMICS CORP","secTicker":"GD"},"LHX":{"cik":"202058","company":"L3HARRIS TECHNOLOGIES, INC. /DE/","secTicker":"LHX"},"HII":{"cik":"1501585","company":"HUNTINGTON INGALLS INDUSTRIES, INC.","secTicker":"HII"},"BA":{"cik":"12927","company":"BOEING CO","secTicker":"BA"},"TXT":{"cik":"217346","company":"TEXTRON INC","secTicker":"TXT"},"TDG":{"cik":"1260221","company":"TransDigm Group INC","secTicker":"TDG"},"HEI":{"cik":"46619","company":"HEICO CORP","secTicker":"HEI"},"CW":{"cik":"26324","company":"CURTISS WRIGHT CORP","secTicker":"CW"},"LDOS":{"cik":"1336920","company":"Leidos Holdings, Inc.","secTicker":"LDOS"},"BAH":{"cik":"1443646","company":"Booz Allen Hamilton Holding Corp","secTicker":"BAH"},"CACI":{"cik":"16058","company":"CACI INTERNATIONAL INC /DE/","secTicker":"CACI"},"SAIC":{"cik":"1571123","company":"Science Applications International Corp","secTicker":"SAIC"},"PSN":{"cik":"275880","company":"PARSONS CORP","secTicker":"PSN"},"KBR":{"cik":"1357615","company":"KBR, INC.","secTicker":"KBR"},"J":{"cik":"52988","company":"JACOBS SOLUTIONS INC.","secTicker":"J"},"ACM":{"cik":"868857","company":"AECOM","secTicker":"ACM"},"EME":{"cik":"105634","company":"EMCOR Group, Inc.","secTicker":"EME"},"PWR":{"cik":"1050915","company":"QUANTA SERVICES, INC.","secTicker":"PWR"},"MAS":{"cik":"62996","company":"MASCO CORP /DE/","secTicker":"MAS"},"BLDR":{"cik":"1316835","company":"Builders FirstSource, Inc.","secTicker":"BLDR"},"URI":{"cik":"1067701","company":"UNITED RENTALS, INC.","secTicker":"URI"},"FAST":{"cik":"815556","company":"FASTENAL CO","secTicker":"FAST"},"GWW":{"cik":"277135","company":"W.W. GRAINGER, INC.","secTicker":"GWW"},"ETN":{"cik":"1551182","company":"Eaton Corp plc","secTicker":"ETN"},"EMR":{"cik":"32604","company":"EMERSON ELECTRIC CO","secTicker":"EMR"},"ROK":{"cik":"1024478","company":"ROCKWELL AUTOMATION, INC","secTicker":"ROK"},"PH":{"cik":"76334","company":"Parker-Hannifin Corp","secTicker":"PH"},"DOV":{"cik":"29905","company":"DOVER Corp","secTicker":"DOV"},"IEX":{"cik":"832101","company":"IDEX CORP /DE/","secTicker":"IEX"},"XYL":{"cik":"1524472","company":"Xylem Inc.","secTicker":"XYL"},"AME":{"cik":"1037868","company":"AMETEK INC/","secTicker":"AME"},"XOM":{"cik":"2115436","company":"ExxonMobil Holdings Corp","secTicker":"XOM"},"CVX":{"cik":"93410","company":"CHEVRON CORP","secTicker":"CVX"},"COP":{"cik":"1163165","company":"CONOCOPHILLIPS","secTicker":"COP"},"EOG":{"cik":"821189","company":"EOG RESOURCES INC","secTicker":"EOG"},"EXE":{"cik":"895126","company":"EXPAND ENERGY Corp","secTicker":"EXE"},"APA":{"cik":"1841666","company":"APA Corp","secTicker":"APA"},"BKR":{"cik":"1701605","company":"Baker Hughes Co","secTicker":"BKR"},"NOV":{"cik":"1021860","company":"NOV Inc.","secTicker":"NOV"},"FTI":{"cik":"1681459","company":"TechnipFMC plc","secTicker":"FTI"},"CHRD":{"cik":"1486159","company":"Chord Energy Corp","secTicker":"CHRD"},"MTDR":{"cik":"1520006","company":"Matador Resources Co","secTicker":"MTDR"},"AR":{"cik":"1433270","company":"ANTERO RESOURCES Corp","secTicker":"AR"},"RRC":{"cik":"315852","company":"RANGE RESOURCES CORP","secTicker":"RRC"},"EQT":{"cik":"33213","company":"EQT Corp","secTicker":"EQT"},"LNG":{"cik":"3570","company":"Cheniere Energy, Inc.","secTicker":"LNG"},"OKE":{"cik":"1039684","company":"ONEOK INC /NEW/","secTicker":"OKE"},"WMB":{"cik":"107263","company":"WILLIAMS COMPANIES, INC.","secTicker":"WMB"},"KMI":{"cik":"1506307","company":"KINDER MORGAN, INC.","secTicker":"KMI"},"TRGP":{"cik":"1389170","company":"Targa Resources Corp.","secTicker":"TRGP"},"NEE":{"cik":"753308","company":"NEXTERA ENERGY INC","secTicker":"NEE"},"DUK":{"cik":"1326160","company":"Duke Energy CORP","secTicker":"DUK"},"SO":{"cik":"92122","company":"SOUTHERN CO","secTicker":"SO"},"D":{"cik":"715957","company":"DOMINION ENERGY, INC","secTicker":"D"},"AEP":{"cik":"4904","company":"AMERICAN ELECTRIC POWER CO INC","secTicker":"AEP"},"EXC":{"cik":"1109357","company":"EXELON CORP","secTicker":"EXC"},"XEL":{"cik":"72903","company":"XCEL ENERGY INC","secTicker":"XEL"},"ED":{"cik":"1047862","company":"CONSOLIDATED EDISON INC","secTicker":"ED"},"WEC":{"cik":"783325","company":"WEC ENERGY GROUP, INC.","secTicker":"WEC"},"ES":{"cik":"72741","company":"EVERSOURCE ENERGY","secTicker":"ES"},"PEG":{"cik":"788784","company":"PUBLIC SERVICE ENTERPRISE GROUP INC","secTicker":"PEG"},"SRE":{"cik":"1032208","company":"SEMPRA","secTicker":"SRE"},"PCG":{"cik":"1004980","company":"PG&E Corp","secTicker":"PCG"},"CEG":{"cik":"1868275","company":"Constellation Energy Corp","secTicker":"CEG"},"VST":{"cik":"1692819","company":"Vistra Corp.","secTicker":"VST"},"NRG":{"cik":"1013871","company":"NRG ENERGY, INC.","secTicker":"NRG"},"TLN":{"cik":"1622536","company":"Talen Energy Corp","secTicker":"TLN"},"AES":{"cik":"874761","company":"AES CORP","secTicker":"AES"},"ETR":{"cik":"65984","company":"ENTERGY CORP /DE/","secTicker":"ETR"},"FE":{"cik":"1031296","company":"FIRSTENERGY CORP","secTicker":"FE"},"CNP":{"cik":"1130310","company":"CENTERPOINT ENERGY INC","secTicker":"CNP"},"CMS":{"cik":"811156","company":"CMS ENERGY CORP","secTicker":"CMS"},"WMT":{"cik":"104169","company":"Walmart Inc.","secTicker":"WMT"},"TGT":{"cik":"27419","company":"TARGET CORP","secTicker":"TGT"},"DG":{"cik":"29534","company":"DOLLAR GENERAL CORP","secTicker":"DG"},"DLTR":{"cik":"935703","company":"DOLLAR TREE, INC.","secTicker":"DLTR"},"KR":{"cik":"56873","company":"KROGER CO","secTicker":"KR"},"ROST":{"cik":"745732","company":"ROSS STORES, INC.","secTicker":"ROST"},"TJX":{"cik":"109198","company":"TJX COMPANIES INC /DE/","secTicker":"TJX"},"BURL":{"cik":"1579298","company":"Burlington Stores, Inc.","secTicker":"BURL"},"ULTA":{"cik":"1403568","company":"Ulta Beauty, Inc.","secTicker":"ULTA"},"DKS":{"cik":"1089063","company":"DICK'S SPORTING GOODS, INC.","secTicker":"DKS"},"BBY":{"cik":"764478","company":"BEST BUY CO INC","secTicker":"BBY"},"AZO":{"cik":"866787","company":"AUTOZONE INC","secTicker":"AZO"},"ORLY":{"cik":"898173","company":"O REILLY AUTOMOTIVE INC","secTicker":"ORLY"},"AAP":{"cik":"1158449","company":"ADVANCE AUTO PARTS INC","secTicker":"AAP"},"TSCO":{"cik":"916365","company":"TRACTOR SUPPLY CO /DE/","secTicker":"TSCO"},"NKE":{"cik":"320187","company":"NIKE, Inc.","secTicker":"NKE"},"LULU":{"cik":"1397187","company":"lululemon athletica inc.","secTicker":"LULU"},"DECK":{"cik":"910521","company":"DECKERS OUTDOOR CORP","secTicker":"DECK"},"CROX":{"cik":"1334036","company":"Crocs, Inc.","secTicker":"CROX"},"TPR":{"cik":"1116132","company":"TAPESTRY, INC.","secTicker":"TPR"},"RL":{"cik":"1037038","company":"RALPH LAUREN CORP","secTicker":"RL"},"PVH":{"cik":"78239","company":"PVH CORP. /DE/","secTicker":"PVH"},"VFC":{"cik":"103379","company":"V F CORP","secTicker":"VFC"},"STLD":{"cik":"1022671","company":"STEEL DYNAMICS INC","secTicker":"STLD"},"MP":{"cik":"1801368","company":"MP Materials Corp. / DE","secTicker":"MP"},"LIN":{"cik":"1707925","company":"LINDE PLC","secTicker":"LIN"},"APD":{"cik":"2969","company":"Air Products & Chemicals, Inc.","secTicker":"APD"},"SHW":{"cik":"89800","company":"SHERWIN WILLIAMS CO","secTicker":"SHW"},"ECL":{"cik":"31462","company":"ECOLAB INC.","secTicker":"ECL"},"DD":{"cik":"1666700","company":"DuPont de Nemours, Inc.","secTicker":"DD"},"DOW":{"cik":"1751788","company":"DOW INC.","secTicker":"DOW"},"LYB":{"cik":"1489393","company":"LyondellBasell Industries N.V.","secTicker":"LYB"},"PPG":{"cik":"79879","company":"PPG INDUSTRIES INC","secTicker":"PPG"},"ALB":{"cik":"915913","company":"ALBEMARLE CORP","secTicker":"ALB"},"CE":{"cik":"1306830","company":"Celanese Corp","secTicker":"CE"},"EMN":{"cik":"915389","company":"EASTMAN CHEMICAL CO","secTicker":"EMN"},"CF":{"cik":"1324404","company":"CF Industries Holdings, Inc.","secTicker":"CF"},"MOS":{"cik":"1285785","company":"MOSAIC CO","secTicker":"MOS"},"NTR":{"cik":"1725964","company":"Nutrien Ltd.","secTicker":"NTR"},"IP":{"cik":"51434","company":"INTERNATIONAL PAPER CO /NEW/","secTicker":"IP"},"PKG":{"cik":"75677","company":"PACKAGING CORP OF AMERICA","secTicker":"PKG"}},"snapshotSha256":"09c9893eafd931a0d143277be1272b7073d7fe72e89525930e8baae94d21223c"});

const DECLARED = {
  "version": "v6",
  "name": "Investor_AI point-in-time research roster with executable exclusions",
  "createdAt": "2026-09-01",
  "supersedes": "v5",
  "rationale": "The 304-name eligible roster is re-frozen after replacing seven acquired or delisted issuers that no longer resolve in the current SEC ticker map. Breadth can reduce the variance of a portfolio-day observation but cannot make independent trading days arrive faster. Membership is declared once; machine-enforced exclusions and dynamic data-quality/liquidity eligibility are separate and auditable.",
  "exclusions": {
    "binary_biopharma": "Unscheduled FDA/Phase 3 readouts gap 40%+ and no blackout calendar or stop can protect against them. Structurally incompatible with hundreds of position-exposures a year on an edge of tens of basis points.",
    "commodity_beta": "OXY DVN FANG SLB HAL FCX NUE AA CLF - track WTI/copper/steel. Systematic risk wearing a company name; no company-specific event for the evidence engine to find.",
    "rate_driven_financials": "JPM BAC GS MS SCHW BLK - flattest tails in the market but they trade on the curve, not on their own filings.",
    "flat_tail_defensives": "COST HD LOW - Home Depot's implied earnings move was +/-1.8%. Nothing to trade.",
    "managed_care": "UNH CVS CI - beat the options-implied move 88% of the time, the highest in a 29-stock study. Policy shocks are chronically underpriced AND unschedulable - the one gap type a blackout cannot fix.",
    "adrs": "Excluded by the plan's universe boundary until ADR ratio, depositary fee, withholding and termination treatment are implemented."
  },
  "deadTickers": {
    "X": "US Steel \u2014 delisted June 2025 (Nippon Steel)",
    "PARA": "Ticker recycled - now Banzai International, a ~$4M microcap. Paramount Skydance is PSKY.",
    "EA": "take-private closed Aug 2026",
    "MASI": "Danaher acquisition closed; delisted from Nasdaq 2026-06-10.",
    "KLAC_NOTE": "KLAC is live but executed a 10-for-1 split ~May 2026 - unadjusted history shows a spurious -90% gap.",
    "PXD": "Pioneer \u2014 acquired by ExxonMobil, closed 2024",
    "MRO": "Marathon Oil \u2014 acquired by ConocoPhillips, closed 2024",
    "WRK": "WestRock \u2014 merged into Smurfit Westrock (SW), 2024",
    "FISV": "Fiserv \u2014 ticker changed to FI",
    "SWN": "Southwestern \u2014 merged into Expand Energy (EXE), 2024",
    "GPS": "Gap \u2014 ticker changed to GAP",
    "PSTG": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by FFIV.",
    "JNPR": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by LITE.",
    "ANSS": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by BSY.",
    "DFS": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by IBKR.",
    "HES": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by EXE.",
    "CTRA": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by EQT.",
    "SKX": "Removed from v6 after the symbol disappeared from the 2026-09-01 SEC ticker map; replaced by TPR."
  },
  "liquidityNote": "advUsd is computed per cycle from measured price x volume. The gate blocks anything under the floor in _investorStrategy.js. No volume figure is asserted in this file.",
  "tradeTier": [
    {
      "symbol": "NVDA",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MU",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "INTC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AVGO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "QCOM",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TXN",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRVL",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ADI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NXPI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCHP",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ON",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SWKS",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "QRVO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MPWR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LSCC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALAB",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRDO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMKR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ONTO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ENTG",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MKSI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ACLS",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UCTT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ICHR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMAT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "LRCX",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KLAC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TER",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COHR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WOLF",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SLAB",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SITM",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "POWI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DIOD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DELL",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HPQ",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HPE",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SMCI",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "WDC",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STX",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTAP",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FFIV",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ANET",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CSCO",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LITE",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VRT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NVT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GEV",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZBRA",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KEYS",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TDY",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TRMB",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FLEX",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JBL",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SANM",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BHE",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PLXS",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FN",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MSFT",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ORCL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "CRM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ADBE",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "INTU",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "WDAY",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PANW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRWD",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ZS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OKTA",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NET",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DDOG",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MDB",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TEAM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HUBS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VEEV",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TYL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PTC",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BSY",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CDNS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNPS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ADSK",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROP",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FTNT",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GEN",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOCU",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TWLO",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BILL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PCTY",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PAYC",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMZN",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GOOGL",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "META",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NFLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "DIS",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SPOT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RBLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TTWO",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TTD",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PINS",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNAP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RDDT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UBER",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "LYFT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DASH",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABNB",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BKNG",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXPE",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETSY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EBAY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CHWY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "W",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SHOP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JPM",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAC",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WFC",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "C",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SCHW",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BLK",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BX",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KKR",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APO",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ARES",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COIN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "HOOD",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SOFI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PYPL",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "V",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MA",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AXP",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COF",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IBKR",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SYF",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FIS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GPN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ICE",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CME",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NDAQ",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SPGI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCO",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MSCI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TROW",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BEN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IVZ",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STT",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTRS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UNH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ELV",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CI",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CVS",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HUM",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CNC",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCK",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COR",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CAH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BSX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SYK",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MDT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ISRG",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EW",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DXCM",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PODD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZBH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BDX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RMD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALGN",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TMO",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DHR",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "A",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WAT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MTD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IQV",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRL",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HCA",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UHS",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "THC",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DVA",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LLY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRK",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PFE",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABBV",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BMY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMGN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GILD",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VRTX",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "REGN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BIIB",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRNA",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "INCY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NBIX",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALNY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BMRN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXEL",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UTHR",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JAZZ",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HALO",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SRPT",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IONS",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RARE",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FOLD",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ITCI",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AXSM",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CORT",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CAT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DE",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HON",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GE",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RTX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LMT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOC",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GD",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LHX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HII",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BA",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TXT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TDG",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HEI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CW",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LDOS",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAH",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CACI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SAIC",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PSN",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KBR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "J",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ACM",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EME",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PWR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MAS",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BLDR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "URI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FAST",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GWW",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETN",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EMR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROK",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PH",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOV",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IEX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XYL",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AME",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XOM",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CVX",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COP",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OXY",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DVN",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FANG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EOG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXE",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APA",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SLB",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HAL",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BKR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOV",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FTI",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CHRD",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MTDR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RRC",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EQT",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LNG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OKE",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WMB",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KMI",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TRGP",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NEE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DUK",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SO",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "D",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AEP",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXC",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XEL",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ED",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WEC",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ES",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PEG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SRE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PCG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CEG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VST",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NRG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TLN",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AES",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETR",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CNP",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CMS",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WMT",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COST",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TGT",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HD",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LOW",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DG",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DLTR",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KR",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROST",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TJX",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BURL",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ULTA",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DKS",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BBY",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AZO",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ORLY",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AAP",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TSCO",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NKE",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LULU",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DECK",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CROX",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TPR",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RL",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PVH",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VFC",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FCX",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NUE",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STLD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CLF",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AA",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MP",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LIN",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SHW",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ECL",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOW",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LYB",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PPG",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALB",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CE",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EMN",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CF",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MOS",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTR",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IP",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PKG",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    }
  ],
  "researchTier": [
    {
      "symbol": "AMD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MU",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MRVL",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "AMAT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "SMCI",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "VRT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ORCL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "CRWD",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NFLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "UBER",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "COIN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    }
  ],
  "selfCorrection": "Bootstrap resolves identifiers against SEC's ticker map. Unresolved identifiers fail the freeze rather than silently shrinking an experiment; later eligibility changes are recorded separately from immutable membership."
};

const EXCLUDED = {
  OXY:"commodity_beta",DVN:"commodity_beta",FANG:"commodity_beta",SLB:"commodity_beta",HAL:"commodity_beta",
  FCX:"commodity_beta",NUE:"commodity_beta",AA:"commodity_beta",CLF:"commodity_beta",
  JPM:"rate_driven_financials",BAC:"rate_driven_financials",GS:"rate_driven_financials",MS:"rate_driven_financials",
  SCHW:"rate_driven_financials",BLK:"rate_driven_financials",
  UNH:"managed_care",CVS:"managed_care",CI:"managed_care",
  COST:"flat_tail_defensives",HD:"flat_tail_defensives",LOW:"flat_tail_defensives",
  MRNA:"binary_biopharma",BIIB:"binary_biopharma",INCY:"binary_biopharma",NBIX:"binary_biopharma",
  ALNY:"binary_biopharma",BMRN:"binary_biopharma",EXEL:"binary_biopharma",UTHR:"binary_biopharma",
  JAZZ:"binary_biopharma",HALO:"binary_biopharma",SRPT:"binary_biopharma",IONS:"binary_biopharma",
  RARE:"binary_biopharma",FOLD:"binary_biopharma",ITCI:"binary_biopharma",AXSM:"binary_biopharma",CORT:"binary_biopharma",
};
const declaredTradeTier = DECLARED.tradeTier;
const excludedTier = declaredTradeTier.filter((r)=>EXCLUDED[r.symbol]).map((r)=>({...r,exclusionReason:EXCLUDED[r.symbol]}));
function attachIdentity(row) {
  const identity = IDENTITY.companies && IDENTITY.companies[row.symbol];
  return identity ? { ...row, cik: identity.cik, company: identity.company,
    cikSource: "sec_company_tickers", companySource: "sec_company_tickers" } : { ...row };
}
const tradeTier = declaredTradeTier.filter((r)=>!EXCLUDED[r.symbol]).map(attachIdentity);
const researchTier = DECLARED.researchTier.map(attachIdentity);
module.exports = {...DECLARED,immutable:true,declaredTradeTierCount:declaredTradeTier.length,
  tradeTier,researchTier,excludedTier,
  identitySnapshot:{schema:IDENTITY.schema,source:IDENTITY.source,count:IDENTITY.count,
    snapshotSha256:IDENTITY.snapshotSha256},
  enforcement:{eligibleCount:tradeTier.length,excludedCount:excludedTier.length,
    exclusionPolicyVersion:"material-exclusions-v1"}};
