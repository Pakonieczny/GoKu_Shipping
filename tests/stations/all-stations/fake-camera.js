/* A phone's camera for the scanner pages (weld-scan-1, assembly-scan-N, shipping-scan-N, sort-scan ...), made of a canvas:
 * navigator.mediaDevices.getUserMedia answers a stream whose picture is the QR code of window.__showQr(text), drawn with the PAGE's own
 * qrcode.js. The scanner page then reads it with its own jsQR exactly as it reads a real camera, and sends what it decoded through its own code.
 * Injected as an init script into the scanner pages only (the test does not touch the page's functions). */
(function () {
  "use strict";
  var rst = window.__rst || window.setTimeout.bind(window);
  var W = 640, H = 480, cv = document.createElement("canvas"); cv.width = W; cv.height = H;
  var g = cv.getContext("2d"), shown = "", tick = 0, qrCanvas = null;
  function paint() {
    g.fillStyle = "#fff"; g.fillRect(0, 0, W, H);
    if (qrCanvas) { var s = 360; g.imageSmoothingEnabled = false; g.drawImage(qrCanvas, (W - s) / 2, (H - s) / 2, s, s); }
    g.fillStyle = (tick++ % 2) ? "#fefefe" : "#fdfdfd"; g.fillRect(0, 0, 2, 2);          // a changing pixel: the stream keeps sending frames
    rst(paint, 120);
  }
  window.__showQr = function (text) {
    shown = String(text || ""); qrCanvas = null;
    if (!shown) return true;
    try {
      var host = document.createElement("div"); host.style.cssText = "position:fixed;left:-9999px;top:0;";
      document.body.appendChild(host);
      new window.QRCode(host, { text: shown, width: 360, height: 360, correctLevel: window.QRCode.CorrectLevel ? window.QRCode.CorrectLevel.M : 0 });
      qrCanvas = host.querySelector("canvas");
      if (!qrCanvas || !qrCanvas.width) { var im = host.querySelector("img"); if (im) { var c2 = document.createElement("canvas"); c2.width = 360; c2.height = 360; c2.getContext("2d").drawImage(im, 0, 0, 360, 360); qrCanvas = c2; } }
      return !!qrCanvas;
    } catch (e) { return false; }
  };
  window.__camera = { shown: function () { return shown; } };
  paint();
  var stream = null;
  var md = navigator.mediaDevices || (navigator.mediaDevices = {});
  md.getUserMedia = function () { if (!stream) stream = cv.captureStream(15); return Promise.resolve(stream); };
  md.enumerateDevices = function () { return Promise.resolve([{ kind: "videoinput", label: "test camera", deviceId: "cam" }]); };
})();
