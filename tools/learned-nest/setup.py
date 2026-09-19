"""Fetch pinned upstream research modules and extract a separately downloaded checkpoint.
No training dataset or arbitrary pickle objects are loaded. See docs/charm-nest-learned.md.
"""
import argparse
import hashlib
import urllib.request
import zipfile
from pathlib import Path

REVISION = "402a9f9a44f2c501f19a4291fca15f8acc943902"
DIGESTS = {
    "score_model.py": "b58c75b2c1df71175e31b8a08dc19771aecfda38a3ef1dddd3c7ca1035ed9368",
    "gnn_feature.py": "89145ba747be55ebe5799a8676cd3088176d8c07954f8d87ca3f37a6e9a94344",
    "sde.py": "8a1643b1127393579843caad53c3cf8415b70f17d789333c177476c6062cc3ea",
}
WEIGHTS = "e3585572100e61c5f3112a8925e7add420b70c8c7a8f23e29be97788ad3c5643"


def checked(data, digest):
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("Downloaded file does not match the reviewed version")
    return data


def setup(archive, destination):
    dest = Path(destination).resolve()
    (dest / "src").mkdir(parents=True, exist_ok=True)
    for name, digest in DIGESTS.items():
        url = f"https://raw.githubusercontent.com/TimHsue/GFPack-pp/{REVISION}/src/{name}"
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read()
        # Normalize the final newline consistently with the reviewed GitHub contents.
        data = data.replace(b"\r\n", b"\n").rstrip(b"\n") + b"\n"
        (dest / "src" / name).write_bytes(checked(data, digest))
    (dest / "src" / "__init__.py").write_text("")
    with zipfile.ZipFile(archive) as z:
        info = z.getinfo("data/ckpt/dental.pth")
        if info.file_size > 100_000_000:
            raise ValueError("Unexpected checkpoint size")
        data = checked(z.read(info), WEIGHTS)
    (dest / "dental.pth").write_bytes(data)
    print(f"Verified model files ready in {dest}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("archive", help="Authors' downloaded gfpp.zip archive")
    p.add_argument("--destination", default=str(Path(__file__).parent / "research"))
    a = p.parse_args()
    setup(a.archive, a.destination)
