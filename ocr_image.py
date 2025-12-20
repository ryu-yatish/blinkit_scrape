"""
Quick helper script to run Google Vision OCR (TEXT_DETECTION) against a local
image file or an image URL using the existing service account credentials.

Usage:
  python ocrtest.py --image path/to/file.jpg
  python ocrtest.py --url https://example.com/image.jpg

Optional flags:
  --creds <path>   Override the service-account JSON (defaults to the one in
                   this folder: nutrisnap-82709-firebase-adminsdk-fbsvc-48a3722abf.json)

Requires:
  pip install google-cloud-vision requests pillow
"""

from __future__ import annotations

import argparse
import json
from io import BytesIO
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Tuple

import requests
from google.cloud import vision
from google.protobuf.json_format import MessageToDict
from PIL import Image


DEFAULT_CREDS = Path(__file__).parent / "nutrisnap-82709-firebase-adminsdk-fbsvc-48a3722abf.json"


def detect_text(
    image_path: Path, creds_path: Path = DEFAULT_CREDS
) -> Tuple[str, List[Dict[str, Any]]]:
    """Run TEXT_DETECTION on the given image and return the full text + raw annotations."""
    client = vision.ImageAnnotatorClient.from_service_account_file(str(creds_path))

    with image_path.open("rb") as f:
        content = f.read()

    image = vision.Image(content=content)
    response = client.text_detection(image=image)

    if response.error.message:
        raise RuntimeError(f"OCR failed: {response.error.message}")
    else:
        print("OCR successful")
    annotations = response.text_annotations or []
    full_text = annotations[0].description if annotations else ""

    # Convert protobuf annotations to plain dicts for easier inspection/serialization.
    annotation_dicts = [MessageToDict(ann._pb) for ann in annotations]

    return full_text, annotation_dicts


def detect_text_from_url(
    image_url: str, creds_path: Path = DEFAULT_CREDS
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Download the remote image locally first, then run TEXT_DETECTION on it.

    This mirrors the local file flow but ensures the image is stored on disk
    before sending to the Vision API.
    """
    # Download image to a temporary file so the local file pipeline is reused.
    resp = requests.get(image_url, timeout=15)
    resp.raise_for_status()

    # Inspect the payload to see what actually arrived.
    content_type = resp.headers.get("Content-Type", "unknown")
    declared_length = resp.headers.get("Content-Length")
    actual_size = len(resp.content)
    sniff_format = "unknown"
    width = height = None
    sniff_error = None
    try:
        with Image.open(BytesIO(resp.content)) as img:
            sniff_format = img.format or "unknown"
            width, height = img.size
    except Exception as exc:
        sniff_error = str(exc)

    print("Downloaded image metadata:")
    print(f"  URL: {image_url}")
    print(f"  HTTP Content-Type: {content_type}")
    if declared_length:
        print(f"  Content-Length header: {declared_length} bytes")
    print(f"  Actual bytes downloaded: {actual_size} bytes")
    if width and height:
        print(f"  Detected format: {sniff_format}, dimensions: {width}x{height}")
    else:
        print(f"  Detected format: {sniff_format}")
        if sniff_error:
            print(f"  (Could not read dimensions: {sniff_error})")

    suffix = Path(image_url.split("?")[0]).suffix or ".img"
    tmp_path: Path | None = None

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(resp.content)
        tmp_path = Path(tmp.name)

    try:
        return detect_text(tmp_path, creds_path)
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                # If cleanup fails, we still return the OCR result.
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Google Vision OCR (TEXT_DETECTION)")
    parser.add_argument(
        "--image",
        required=False,
        type=Path,
        help="Path to the image file to OCR",
    )
    parser.add_argument(
        "--url",
        required=False,
        type=str,
        help="URL of the image to OCR",
    )
    parser.add_argument(
        "--creds",
        type=Path,
        default=DEFAULT_CREDS,
        help="Path to the service account JSON (defaults to bundled file)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print raw annotation JSON instead of just the detected text",
    )
    args = parser.parse_args()

    if args.image:
        full_text, annotations = detect_text(args.image, args.creds)
    elif args.url:
        full_text, annotations = detect_text_from_url(args.url, args.creds)
    else:
        raise ValueError("Either --image or --url must be provided")

    if args.json:
        payload = {"text": full_text, "annotations": annotations}
        print(json.dumps(payload, indent=2))
        Path("annotations.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        print(full_text.strip())


if __name__ == "__main__":
    main()
