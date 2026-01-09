#!/usr/bin/env python3
"""
Upload NutriSnap data starting from previously saved Blinkit combined reports.

This skips fresh scraping and instead reuses the JSON files produced by
blinkit_batch_scraper.py, re-running OCR/barcode detection (when needed) and
uploading nutrition data to NutriSnap.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

import ocr_image as ocr
from blinkit_batch_scraper import (
    DEFAULT_OUTPUT_DIR,
    fetch_existing_product_names,
    product_exists_in_registry,
    run_barcode_on_images,
    run_ocr_on_images,
    upload_product_to_nutrisnap,
    write_json_report,
)

DEFAULT_INPUT_DIR = Path("scrappedData")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload Blinkit products using saved combined reports instead of scraping. "
            "Runs barcode/OCR (optionally re-running even if present) and uploads to "
            "NutriSnap."
        )
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory containing saved combined JSON reports (default: scrappedData).",
    )
    parser.add_argument(
        "--input-file",
        "-f",
        action="append",
        type=Path,
        help=(
            "Specific combined report(s) to process. Can be provided multiple times. "
            "When omitted, all blinkit_combined_report_*.json files in --input-dir are used."
        ),
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="Optional limit on total products to process across all input files.",
    )
    parser.add_argument(
        "--output-dir",
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the new timestamped JSON report will be written.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip uploads to NutriSnap; still runs OCR/barcode and writes a report.",
    )
    parser.add_argument(
        "--ocr-creds",
        type=Path,
        default=ocr.DEFAULT_CREDS,
        help="Path to the service-account JSON used for OCR.",
    )
    parser.add_argument(
        "--force-ocr",
        action="store_true",
        help="Re-run OCR even when ocr_text already exists in the saved report.",
    )
    parser.add_argument(
        "--force-barcode",
        action="store_true",
        help="Re-run barcode detection even when a barcode already exists.",
    )
    return parser.parse_args()


def _dedupe_paths(paths: Iterable[Path]) -> List[Path]:
    seen: set[Path] = set()
    unique: List[Path] = []
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def discover_input_files(input_dir: Path, explicit_files: Optional[List[Path]]) -> List[Path]:
    if explicit_files:
        candidates = _dedupe_paths(explicit_files)
    else:
        candidates = sorted(
            (input_dir.expanduser().resolve()).glob("blinkit_combined_report_*.json")
        )
    files = [path for path in candidates if path.is_file()]
    if not files:
        raise FileNotFoundError(
            "No input JSON files found. Provide --input-file or populate the input directory."
        )
    return files


def _normalize_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_nutrition(entries: Any) -> List[Tuple[str, str]]:
    """
    Convert serialized nutrition entries (list of dicts) back into label/value pairs.
    Accepts tuples too in case the data is already in that format.
    """
    normalized: List[Tuple[str, str]] = []
    if not entries or not isinstance(entries, Iterable):
        return normalized
    for item in entries:
        label: Optional[str]
        value: Optional[str]
        if isinstance(item, dict):
            label = _normalize_str(item.get("label"))
            value = _normalize_str(item.get("value"))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            label = _normalize_str(item[0])
            value = _normalize_str(item[1])
        else:
            continue
        if label and value:
            normalized.append((label, value))
    return normalized


def deserialize_product(raw: Dict[str, Any]) -> Dict[str, Any]:
    detail_raw = raw.get("detail") or {}
    hero_images = [
        url for url in (detail_raw.get("hero_images") or []) if _normalize_str(url)
    ]
    product_record: Dict[str, Any] = {
        "listing_name": _normalize_str(raw.get("listing_name")),
        "listing_image": _normalize_str(raw.get("listing_image")),
        "product_link": _normalize_str(raw.get("product_link")),
        "detail": {
            "detail_name": _normalize_str(detail_raw.get("detail_name")),
            "hero_images": hero_images,
            "nutrition": _normalize_nutrition(detail_raw.get("nutrition")),
            "ingredients": _normalize_str(detail_raw.get("ingredients")),
            "description": _normalize_str(detail_raw.get("description")),
            "fssai_license": _normalize_str(detail_raw.get("fssai_license")),
            "ocr_text": _normalize_str(detail_raw.get("ocr_text")),
            "barcode": _normalize_str(detail_raw.get("barcode")),
            "error": _normalize_str(detail_raw.get("error")),
        },
    }
    return product_record


def load_products_from_file(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_products = payload.get("products") or []
    products: List[Dict[str, Any]] = []
    for raw_product in raw_products:
        if isinstance(raw_product, dict):
            products.append(deserialize_product(raw_product))
    return products


def run_pipeline(args: argparse.Namespace) -> Path:
    input_dir = args.input_dir.expanduser().resolve()
    ocr_creds_path = args.ocr_creds.expanduser().resolve()

    input_files = discover_input_files(input_dir, args.input_file)
    print(f"Found {len(input_files)} input file(s) to process.")

    all_products: List[Dict[str, Any]] = []
    for path in input_files:
        products = load_products_from_file(path)
        print(f"  Loaded {len(products)} products from {path.name}")
        all_products.extend(products)

    total_loaded = len(all_products)
    if args.max_products is not None:
        all_products = all_products[: max(args.max_products, 0)]
    if not all_products:
        raise RuntimeError("No products available to process after applying filters.")

    diagnostics: Dict[str, Any] = {
        "source_files": [str(p) for p in input_files],
        "source_products": total_loaded,
        "processed": len(all_products),
        "force_ocr": args.force_ocr,
        "force_barcode": args.force_barcode,
        "dry_run": args.dry_run,
        "max_products": args.max_products,
    }

    combined_results: List[Dict[str, Any]] = []
    with requests.Session() as session:
        existing_names = fetch_existing_product_names(session)
        print(
            f"Fetched {len(existing_names)} existing NutriSnap product names for deduplication."
        )
        for idx, product_record in enumerate(all_products, start=1):
            display_name = (
                product_record.get("listing_name")
                or product_record.get("detail", {}).get("detail_name")
                or product_record.get("product_link")
                or "<unnamed product>"
            )
            print(f"[{idx}/{len(all_products)}] Product: {display_name}")

            if product_exists_in_registry(product_record, existing_names):
                print("  Skipping OCR and upload; product already exists in NutriSnap.")
                combined_results.append(product_record)
                continue

            image_urls: List[str] = []
            listing_image = product_record.get("listing_image")
            if listing_image:
                image_urls.append(listing_image)
            image_urls.extend(product_record["detail"].get("hero_images") or [])

            if args.force_barcode or not product_record["detail"].get("barcode"):
                barcode_value = run_barcode_on_images(image_urls)
                if barcode_value:
                    product_record["detail"]["barcode"] = barcode_value

            if args.force_ocr or not product_record["detail"].get("ocr_text"):
                ocr_texts = run_ocr_on_images(image_urls, ocr_creds_path)
                if ocr_texts:
                    product_record["detail"]["ocr_text"] = "\n\n".join(ocr_texts)

            combined_results.append(product_record)
            upload_product_to_nutrisnap(
                product_record, session, existing_names, args.dry_run
            )

    listing_target = f"stored-reports:{','.join(p.name for p in input_files)}"
    output_dir = args.output_dir.expanduser().resolve()
    return write_json_report(output_dir, listing_target, diagnostics, combined_results)


def main() -> int:
    args = parse_args()
    try:
        report_path = run_pipeline(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while uploading products from saved reports: {exc}")
        return 1
    print(f"\nCombined report saved to: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


