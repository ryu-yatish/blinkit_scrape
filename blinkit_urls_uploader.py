#!/usr/bin/env python3
"""
Upload Blinkit products from a file of product URLs, following the same flow as
blinkit_batch_scraper.py but skipping the listing-page step.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

import blinkit_single_product_scraper as detail
from blinkit_batch_scraper import (
    fetch_existing_product_names,
    product_exists_in_registry,
    run_barcode_on_images,
    run_ocr_on_images,
    scrape_product_detail,
    upload_product_to_nutrisnap,
    write_json_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Process a list of Blinkit product URLs, run OCR/barcode detection, "
            "and upload nutrition data to NutriSnap."
        )
    )
    parser.add_argument(
        "--urls-file",
        "-f",
        type=Path,
        default=Path("list_urls.txt"),
        help="Path to the text file containing one Blinkit product URL per line.",
    )
    parser.add_argument(
        "--product-timeout",
        type=int,
        default=detail.DEFAULT_TIMEOUT,
        help="Seconds to wait for each product detail page (default: 20).",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="Optional limit on how many URLs to process.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run the product scraper in headless mode (Blinkit may block headless sessions).",
    )
    parser.add_argument(
        "--output-dir",
        "--output",
        "-o",
        type=Path,
        default=Path("scrappedData"),
        help="Directory where the timestamped JSON report will be written.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip uploads to NutriSnap; still writes the JSON report.",
    )
    parser.add_argument(
        "--ocr-creds",
        type=Path,
        default=Path("nutrisnap-82709-firebase-adminsdk-fbsvc-48a3722abf.json"),
        help="Path to the service-account JSON used for OCR.",
    )
    return parser.parse_args()


def load_urls(path: Path) -> List[str]:
    """Return a de-duplicated, order-preserving list of URLs from the file."""
    lines = path.read_text(encoding="utf-8").splitlines()
    seen: Set[str] = set()
    urls: List[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped in seen:
            continue
        seen.add(stripped)
        urls.append(stripped)
    return urls


def build_product_record(url: str, detail_data: Dict[str, Any]) -> Dict[str, Any]:
    hero_images = detail_data.get("hero_images") or []
    listing_name = detail_data.get("detail_name") or None
    listing_image = hero_images[0] if hero_images else None
    return {
        "listing_name": listing_name,
        "listing_image": listing_image,
        "product_link": url,
        "detail": detail_data,
    }


def run_pipeline(args: argparse.Namespace) -> Path:
    urls = load_urls(args.urls_file.expanduser().resolve())
    if args.max_products is not None:
        urls = urls[: max(args.max_products, 0)]

    ocr_creds_path = args.ocr_creds.expanduser().resolve()
    combined_results: List[Dict[str, Any]] = []

    diagnostics: Dict[str, Any] = {
        "source_file": str(args.urls_file),
        "total_urls": len(urls),
    }

    with requests.Session() as session:
        existing_names = fetch_existing_product_names(session)
        print(
            f"Fetched {len(existing_names)} existing NutriSnap product names for deduplication."
        )

        for idx, url in enumerate(urls, start=1):
            print(f"[{idx}/{len(urls)}] Fetching detail page: {url}")
            detail_data = scrape_product_detail(url, args.headless, args.product_timeout)

            product_record = build_product_record(url, detail_data)
            if product_exists_in_registry(product_record, existing_names):
                print("  Skipping OCR and upload; product already exists in NutriSnap.")
                combined_results.append(product_record)
                continue

            image_urls: List[str] = product_record["detail"].get("hero_images") or []
            barcode_value = run_barcode_on_images(image_urls)
            ocr_texts = run_ocr_on_images(image_urls, ocr_creds_path)
            if ocr_texts:
                product_record["detail"]["ocr_text"] = "\n\n".join(ocr_texts)
            if barcode_value:
                product_record["detail"]["barcode"] = barcode_value

            combined_results.append(product_record)
            upload_product_to_nutrisnap(
                product_record, session, existing_names, args.dry_run
            )

    diagnostics["processed"] = len(combined_results)
    listing_target = f"url-list:{args.urls_file}"
    output_dir = args.output_dir.expanduser().resolve()
    return write_json_report(output_dir, listing_target, diagnostics, combined_results)


def main() -> int:
    args = parse_args()
    try:
        report_path = run_pipeline(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while uploading products from URL list: {exc}")
        return 1
    print(f"\nCombined report saved to: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

