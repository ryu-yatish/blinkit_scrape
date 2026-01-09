#!/usr/bin/env python3
"""
Minimal helper to verify barcode decoding with the same logic used in the
batch scraper. Paste one or more image URLs and it will attempt to read a
single barcode (EAN-13, EAN-8, or Codabar) from them.

Usage examples:
  python barcode_test.py --url https://example.com/img.jpg
  python barcode_test.py --url https://img1.jpg https://img2.jpg
  python barcode_test.py  # will prompt for URLs

Dependencies:
  pip install pillow pyzbar requests
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Set

from PIL import Image
from pyzbar.pyzbar import decode as decode_barcode

import combine_images


BARCODE_ALLOWED_TYPES = {
    "EAN13": "ean-13",
    "EAN8": "ean-8",
    "CODABAR": "codabar",
}

DOWNLOAD_TIMEOUT = 15


def _decode_barcode_from_image(image: Image.Image) -> Optional[str]:
    """Return the first allowed barcode value found in the image."""
    try:
        results = decode_barcode(image.convert("L"))
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  Barcode decode failed: {exc}", file=sys.stderr)
        return None

    for result in results:
        fmt = result.type
        if fmt not in BARCODE_ALLOWED_TYPES:
            continue
        value = (result.data or b"").decode("utf-8", errors="ignore").strip()
        if value:
            return value
    return None


def scan_barcode_from_urls(
    image_urls: List[str], timeout: int = DOWNLOAD_TIMEOUT
) -> Optional[str]:
    """
    Attempt to read a single barcode from the provided image URLs.

    Stops at the first valid barcode in allowed formats.
    """
    seen: Set[str] = set()
    for url in image_urls:
        if not url or not isinstance(url, str):
            continue
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        print(f"Scanning barcode in {normalized}")
        try:
            image = combine_images.download_image(normalized, timeout=timeout)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"  Failed to download image for barcode: {exc}", file=sys.stderr)
            continue

        value = _decode_barcode_from_image(image)
        if value:
            print(f"  Found barcode: {value}")
            return value

    print("No allowed barcode found in provided images.")
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test barcode decoding against one or more image URLs."
    )
    parser.add_argument(
        "--url",
        "-u",
        nargs="+",
        help="Image URL(s) to scan for a barcode",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DOWNLOAD_TIMEOUT,
        help=f"HTTP timeout per image request in seconds (default: {DOWNLOAD_TIMEOUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    urls = args.url
    if not urls:
        raw = input("Enter one or more image URLs (space separated): ").strip()
        if not raw:
            print("No URLs provided; exiting.")
            return 1
        urls = raw.split()

    result = scan_barcode_from_urls(urls, timeout=args.timeout)
    return 0 if result else 1


if __name__ == "__main__":
    raise SystemExit(main())

