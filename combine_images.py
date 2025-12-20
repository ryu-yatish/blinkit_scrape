"""
Download four image URLs and stack them vertically with a dark border between each.

Usage:
  python combine_images.py <url1> <url2> <url3> <url4>
  python combine_images.py <url1> <url2> <url3> <url4> --border 18 --output stacked.png

Dependencies:
  pip install pillow requests
"""

from __future__ import annotations

import argparse
from io import BytesIO
from pathlib import Path
from typing import Iterable, List, Tuple

import requests
from PIL import Image


DEFAULT_BORDER_PX = 16
DEFAULT_BORDER_COLOR = "#111111"


def parse_hex_color(value: str) -> Tuple[int, int, int]:
    """Parse a hex color string (#RRGGBB) into an RGB tuple."""
    raw = value.strip().lstrip("#")
    if len(raw) != 6 or any(ch not in "0123456789abcdefABCDEF" for ch in raw):
        raise argparse.ArgumentTypeError(f"Invalid color '{value}'. Use hex like #0f0f0f")

    r = int(raw[0:2], 16)
    g = int(raw[2:4], 16)
    b = int(raw[4:6], 16)
    return r, g, b


def download_image(url: str, timeout: int) -> Image.Image:
    """Fetch an image from a URL and return it as an RGBA Pillow Image."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return Image.open(BytesIO(resp.content)).convert("RGBA")


def stack_vertically(
    images: Iterable[Image.Image],
    border_px: int,
    border_color: Tuple[int, int, int],
) -> Image.Image:
    """Stack images top-to-bottom with a solid border between them."""
    imgs: List[Image.Image] = list(images)
    if len(imgs) != 4:
        raise ValueError(f"Expected exactly 4 images, received {len(imgs)}")

    max_width = max(img.width for img in imgs)
    total_height = sum(img.height for img in imgs) + border_px * (len(imgs) - 1)

    # Start with a dark canvas so border and side padding share the same color.
    canvas = Image.new("RGBA", (max_width, total_height), (*border_color, 255))

    y_offset = 0
    for idx, img in enumerate(imgs):
        x_offset = (max_width - img.width) // 2
        canvas.paste(img, (x_offset, y_offset), img)
        y_offset += img.height
        if idx < len(imgs) - 1:
            y_offset += border_px

    return canvas.convert("RGB")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download four images and combine them vertically with a dark border."
    )
    parser.add_argument(
        "urls",
        nargs=4,
        help="Four image URLs (order is top to bottom in the final image)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("combined_vertical.png"),
        help="Output image path (default: combined_vertical.png)",
    )
    parser.add_argument(
        "--border",
        type=int,
        default=DEFAULT_BORDER_PX,
        help=f"Border thickness in pixels between images (default: {DEFAULT_BORDER_PX})",
    )
    parser.add_argument(
        "--color",
        type=parse_hex_color,
        default=parse_hex_color(DEFAULT_BORDER_COLOR),
        help=f"Border color in hex (default: {DEFAULT_BORDER_COLOR})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="HTTP timeout per image request in seconds (default: 15)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.border < 0:
        raise ValueError("Border thickness must be zero or positive")

    images = [download_image(url, args.timeout) for url in args.urls]
    combined = stack_vertically(images, args.border, args.color)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    combined.save(args.output)
    print(f"Combined image saved to {args.output.resolve()}")


if __name__ == "__main__":
    main()

