from pathlib import Path
import httpx

def download_csv(url: str, out_path: Path, timeout: float = 30.0) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        out_path.write_bytes(r.content)
    return out_path