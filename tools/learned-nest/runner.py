"""Loopback-only GFPack++ inference adapter. Research source/weights stay external.

Usage: python runner.py --source /path/to/GFPack-pp --weights /path/to/dental.pth
The browser's exact raster solver remains responsible for feasible manufacturing layouts.
"""
import argparse
import hashlib
import hmac
import json
import math
import os
import secrets
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ORIGINS = {"https://goldenspike.app", "https://brites-charm-sorter.goldenspike.app"}


class Model:
    def __init__(self, source, weights, threads=4):
        import torch
        sys.path.insert(0, str(Path(source).resolve()))
        from src.score_model import PolygonPackingTransformer
        from src.sde import init_sde, pc_sampler_state
        self.torch, self.sample = torch, pc_sampler_state
        torch.set_num_threads(max(1, min(8, threads, (os.cpu_count() or 2) - 2)))
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        _, marginal, self.sde, _ = init_sde("ve")
        self.net = PolygonPackingTransformer(marginal, self.device).to(self.device)
        # Do not execute arbitrary pickle objects from a downloaded checkpoint.
        self.net.load_state_dict(torch.load(weights, map_location="cpu", weights_only=True), strict=True)
        self.net.eval()
        self.digest = hashlib.sha256(Path(weights).read_bytes()).hexdigest()

    def predict(self, request):
        from torch_geometric.data import Data, Batch
        torch = self.torch
        pieces = request.get("pieces", [])
        if not 1 <= len(pieces) <= 160:
            raise ValueError("Model accepts 1–160 pieces per request")
        height = float(request["sheet"]["hPt"])
        if not math.isfinite(height) or not 1 <= height <= 10000:
            raise ValueError("Invalid sheet height")
        scale = 1205.0 / height  # published teacher's strip height; uniform scale only
        graphs = []
        ids = []
        for piece in pieces:
            poly = piece["polygon"]
            if not 3 <= len(poly) <= 256:
                raise ValueError("Each polygon needs 3–256 vertices")
            if any(len(p) != 2 or any(not isinstance(v, (int, float)) or not math.isfinite(v) or abs(v) > 10000 for v in p) for p in poly):
                raise ValueError("Invalid polygon coordinate")
            pts = [(x * scale, y * scale) for x, y in poly]
            features, perimeter, area = [], 0, 0
            for i, (x, y) in enumerate(pts):
                prev, nxt = pts[i - 1], pts[(i + 1) % len(pts)]
                ax, ay, bx, by = prev[0] - x, prev[1] - y, nxt[0] - x, nxt[1] - y
                cross = ax * by - ay * bx
                angle = abs(math.degrees(math.atan2(cross, ax * bx + ay * by)))
                features.append([x, y, 360 - angle if cross < 0 else angle])
                perimeter += math.hypot(bx, by)
                area += x * nxt[1] - nxt[0] * y
            edges = [(i, (i + 1) % len(pts)) for i in range(len(pts))]
            graphs.append(Data(x=torch.tensor(features, dtype=torch.float32),
                               edge_index=torch.tensor(edges + [(b, a) for a, b in edges], dtype=torch.long).t().contiguous(),
                               area=torch.tensor(abs(area) / 2), perm=torch.tensor(perimeter)))
            ids.append(str(piece["id"]))
        if len(set(ids)) != len(ids):
            raise ValueError("Duplicate piece id")
        torch.manual_seed(int(request.get("seed", 1)) % (2 ** 31))
        t0 = time.monotonic()
        with torch.inference_mode():
            _, result = self.sample(self.net, self.sde, len(ids),
                                    torch.arange(len(ids), device=self.device),
                                    Batch.from_data_list(graphs).to(self.device),
                                    torch.zeros(len(ids), device=self.device),
                                    batch_size=2, num_steps=128)
        if not torch.isfinite(result).all():
            raise ValueError("Model produced non-finite coordinates")
        candidates = [[{"id": pid, "cxPt": float(row[0]) / scale,
                        "cyPt": float(row[1]) / scale,
                        "angle": math.degrees(math.atan2(float(row[3]), float(row[2]))) % 360}
                       for pid, row in zip(ids, batch)] for batch in result.cpu()]
        return {"model": "GFPack++ dental pretrained", "weightsSha256": self.digest,
                "device": self.device, "elapsedMs": round((time.monotonic() - t0) * 1000),
                "candidates": candidates}


def serve(model, token, port, origins):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(15)

        def log_message(self, *_):
            pass  # tokens and customer geometry must never enter access logs

        def allowed(self):
            return self.headers.get("Origin") in origins and self.headers.get("Host") in {f"127.0.0.1:{port}", f"localhost:{port}"}

        def reply(self, code, value):
            self.send_response(code)
            if self.allowed():
                self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
                self.send_header("Vary", "Origin")
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(json.dumps(value, allow_nan=False).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_OPTIONS(self):
            if not self.allowed():
                return self.reply(403, {"error": "Origin not allowed"})
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.end_headers()

        def authorized(self):
            return self.allowed() and hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + token)

        def do_GET(self):
            if not self.authorized():
                return self.reply(403, {"error": "Pair this browser with the runner"})
            if self.path != "/health":
                return self.reply(404, {"error": "Not found"})
            self.reply(200, {"ready": True, "model": "GFPack++ dental pretrained", "device": model.device, "weightsSha256": model.digest})

        def do_POST(self):
            if not self.authorized():
                return self.reply(403, {"error": "Pair this browser with the runner"})
            if self.path != "/propose":
                return self.reply(404, {"error": "Not found"})
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 2_000_000:
                    return self.reply(413, {"error": "Request too large"})
                if not lock.acquire(blocking=False):
                    return self.reply(429, {"error": "Runner is finishing another sheet; retry shortly"})
                try:
                    request = json.loads(self.rfile.read(length))
                    result = model.predict(request)
                finally:
                    lock.release()
                self.reply(200, result)
            except (ValueError, TypeError, KeyError) as exc:
                self.reply(400, {"error": str(exc)})
            except Exception:
                self.reply(500, {"error": "Model inference failed; inspect runner setup"})

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--weights", required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--origin", action="append", default=[])
    args = parser.parse_args()
    model = Model(args.source, args.weights, args.threads)
    token = secrets.token_urlsafe(24)
    print(f"GFPack++ ready on http://127.0.0.1:{args.port} ({model.device})\nPairing key: {token}", flush=True)
    serve(model, token, args.port, ORIGINS | set(args.origin))
