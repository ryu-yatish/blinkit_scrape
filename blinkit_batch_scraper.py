#!/usr/bin/env python3
"""
Run the listing scraper to collect Blinkit products, then visit each product page
with the single-product scraper and dump everything into one JSON report.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from PIL import Image
from pyzbar.pyzbar import decode as decode_barcode

import combine_images
import ocr_image as ocr

import blinkit_products_scraper as listing
import blinkit_single_product_scraper as detail

NUTRISNAP_UPLOAD_URL = (
    "https://us-central1-nutrisnap-82709.cloudfunctions.net/api/products/upload-scraped-data"
)
NUTRISNAP_PRODUCTS_NAMES_URL = (
    "https://us-central1-nutrisnap-82709.cloudfunctions.net/api/products/names"
)
DEFAULT_OUTPUT_DIR = Path("scrappedData")
OCR_BATCH_SIZE = 4
OCR_DOWNLOAD_TIMEOUT = 15
OCR_BORDER_COLOR = combine_images.parse_hex_color(combine_images.DEFAULT_BORDER_COLOR)
BARCODE_ALLOWED_TYPES = {
    "EAN13": "ean-13",
    "EAN8": "ean-8",
    "CODABAR": "codabar",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Chain the Blinkit listing scraper with the single-product scraper "
            "and write a combined JSON report."
        )
    )
    parser.add_argument(
        "--list-url",
        "-u",
        default=listing.DEFAULT_TARGET,
        help="Listing page or snapshot to feed into blinkit_products_scraper.",
    )
    parser.add_argument(
        "--listing-timeout",
        type=int,
        default=20,
        help="Seconds to wait for the listing grid (default: 20).",
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
        help="Limit how many products to process from the listing.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run both scrapers in headless mode (Blinkit may block headless sessions).",
    )
    parser.add_argument(
        "--output-dir",
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directory where the timestamped JSON report will be written "
            "(default: scrappedData)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch everything but skip uploads to the NutriSnap API.",
    )
    parser.add_argument(
        "--ocr-creds",
        type=Path,
        default=ocr.DEFAULT_CREDS,
        help=(
            "Path to the service-account JSON used for OCR (defaults to the bundled "
            "Vision API credentials)."
        ),
    )
    return parser.parse_args()


def scrape_listing(
    target: str, headless: bool, timeout: int
) -> Tuple[str, List[Tuple[str, str, Optional[str]]], Dict[str, Any]]:
    normalized = listing.normalize_target(target)
    driver: Optional[listing.webdriver.Chrome] = None
    try:
        driver = listing.build_driver(headless)
        html = listing.fetch_page_source(driver, normalized, timeout)
        products, diagnostics = listing.extract_products(html, normalized)
    finally:
        if driver is not None:
            driver.quit()
    return normalized, products, diagnostics


def scrape_product_detail(
    url: str, headless: bool, timeout: int
) -> Dict[str, Any]:
    driver: Optional[detail.webdriver.Chrome] = None
    try:
        driver = detail.build_driver(headless)
        state = detail.wait_for_preloaded_state(driver, url, timeout)
        (
            product_name,
            hero_images,
            nutrition,
            text_sections,
        ) = detail.extract_product_info(state)
        return {
            "detail_name": product_name,
            "hero_images": hero_images,
            "nutrition": nutrition,
            "ingredients": text_sections.get("ingredients"),
            "description": text_sections.get("description"),
            "fssai_license": text_sections.get("fssai_license"),
            "error": None,
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "detail_name": None,
            "hero_images": [],
            "nutrition": [],
            "ingredients": None,
            "description": None,
            "fssai_license": None,
            "error": str(exc),
        }
    finally:
        if driver is not None:
            driver.quit()


def build_timestamped_output_path(base_dir: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"blinkit_combined_report_{timestamp}.json"


def normalize_name(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def fetch_existing_product_names(session: requests.Session) -> Set[str]:
    try:
        response = session.get(NUTRISNAP_PRODUCTS_NAMES_URL, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:  # pragma: no cover
        print(
            f"Warning: failed to fetch NutriSnap product names: {exc}", file=sys.stderr
        )
        return set()
    except ValueError as exc:  # pragma: no cover
        print(
            f"Warning: unexpected response format when fetching product names: {exc}",
            file=sys.stderr,
        )
        return set()

    names: Set[str] = set()
    if isinstance(payload, list):
        for entry in payload:
            normalized = normalize_name(entry if isinstance(entry, str) else None)
            if normalized:
                names.add(normalized)
    else:
        print(
            "Warning: product names endpoint returned non-list payload; skipping dedupe.",
            file=sys.stderr,
        )
    return names


def serialize_nutrition_entries(
    entries: Optional[List[Tuple[str, str]]],
) -> List[Dict[str, str]]:
    serialized: List[Dict[str, str]] = []
    if not entries:
        return serialized
    for label, value in entries:
        if not label or not value:
            continue
        serialized.append({"label": label, "value": value})
    return serialized


def serialize_product_for_json(product: Dict[str, Any]) -> Dict[str, Any]:
    detail_result = product["detail"]
    return {
        "listing_name": product["listing_name"],
        "listing_image": product["listing_image"],
        "product_link": product.get("product_link"),
        "detail": {
            "detail_name": detail_result.get("detail_name"),
            "hero_images": detail_result.get("hero_images") or [],
            "nutrition": serialize_nutrition_entries(detail_result.get("nutrition")),
            "ingredients": detail_result.get("ingredients"),
            "description": detail_result.get("description"),
            "fssai_license": detail_result.get("fssai_license"),
            "ocr_text": detail_result.get("ocr_text"),
            "barcode": detail_result.get("barcode"),
            "error": detail_result.get("error"),
        },
    }


def write_json_report(
    output_dir: Path,
    listing_target: str,
    diagnostics: Dict[str, Any],
    products: List[Dict[str, Any]],
) -> Path:
    output_path = build_timestamped_output_path(output_dir)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "listing_target": listing_target,
        "diagnostics": diagnostics,
        "products": [serialize_product_for_json(product) for product in products],
    }
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return output_path


def nutrition_pairs_to_dict(
    entries: Optional[List[Tuple[str, str]]]
) -> Optional[Dict[str, str]]:
    if not entries:
        return None
    nutrition: Dict[str, str] = {}
    for label, value in entries:
        if not label or not value:
            continue
        nutrition[label] = value
    return nutrition or None


def normalize_ingredients(ingredients: Optional[str]) -> Optional[List[str]]:
    """
    Split a free-form ingredients string into a list of ingredients.
    Preserves order and drops empty tokens. Returns None when no content.
    """
    if not ingredients or not isinstance(ingredients, str):
        return None
    parts = [part.strip() for part in re.split(r"[,\n;]+", ingredients) if part.strip()]
    return parts or None


def _build_combined_image(image_urls: List[str]) -> Path:
    """Download four images and stack them vertically into a temp PNG for OCR."""
    images = [
        combine_images.download_image(url, timeout=OCR_DOWNLOAD_TIMEOUT)
        for url in image_urls
    ]
    combined = combine_images.stack_vertically(
        images, combine_images.DEFAULT_BORDER_PX, OCR_BORDER_COLOR
    )

    # Close the temp file immediately so Windows can reopen it for saving.
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        combined_path = Path(tmp.name)
    combined.save(combined_path)
    return combined_path


def _run_single_image_ocr(url: str, creds_path: Path) -> Optional[str]:
    """Run OCR on a single image URL and return cleaned text."""
    print(f"Running OCR on {url}")
    try:
        full_text, _ = ocr.detect_text_from_url(url, creds_path)
        cleaned = full_text.strip()
        print(f"OCR result: {cleaned}")
        return cleaned or None
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  OCR failed for image {url}: {exc}", file=sys.stderr)
        return None


def run_ocr_on_images(image_urls: List[str], creds_path: Path) -> List[str]:
    """
    Fetch OCR full text for the provided image URLs.

    - Deduplicates URLs.
    - Combines images in batches of four to cut OCR calls by ~75%.
    - Falls back to per-image OCR if batch combination or OCR fails.
    """
    texts: List[str] = []
    seen: Set[str] = set()
    unique_urls: List[str] = []
    for url in image_urls:
        if not url or not isinstance(url, str):
            continue
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_urls.append(normalized)

    print(f"Running OCR on {len(unique_urls)} unique images")

    idx = 0
    while idx < len(unique_urls):
        batch = unique_urls[idx : idx + OCR_BATCH_SIZE]

        # Attempt combined OCR only for full batches of four.
        if len(batch) == OCR_BATCH_SIZE:
            print(f"Combining {len(batch)} images for batch OCR:")
            for b_url in batch:
                print(f"  - {b_url}")
            combined_path: Optional[Path] = None
            try:
                combined_path = _build_combined_image(batch)
                full_text, _ = ocr.detect_text(combined_path, creds_path)
                cleaned = full_text.strip()
                if cleaned:
                    texts.append(cleaned)
                    print("Batch OCR succeeded.")
                    idx += OCR_BATCH_SIZE
                    continue
                print("Batch OCR returned empty text; falling back to single images.")
            except Exception as exc:  # pylint: disable=broad-except
                print(
                    f"  Combined OCR failed for batch starting at {idx}: {exc}",
                    file=sys.stderr,
                )
            finally:
                if combined_path and combined_path.exists():
                    try:
                        combined_path.unlink(missing_ok=True)
                    except Exception:
                        pass

        # Fall back to single-image OCR for partial batches or failed combined runs.
        for url in batch:
            single_text = _run_single_image_ocr(url, creds_path)
            if single_text:
                texts.append(single_text)
        idx += len(batch)

    return texts


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


def run_barcode_on_images(image_urls: List[str]) -> Optional[str]:
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
            image = combine_images.download_image(normalized, timeout=OCR_DOWNLOAD_TIMEOUT)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"  Failed to download image for barcode: {exc}", file=sys.stderr)
            continue

        value = _decode_barcode_from_image(image)
        if value:
            print(f"  Found barcode: {value}")
            return value

    print("No allowed barcode found in provided images.")
    return None


def build_upload_payload(product: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    detail_result = product["detail"]
    nutrition = nutrition_pairs_to_dict(detail_result.get("nutrition"))
    ingredients_raw = detail_result.get("ingredients")
    ingredients_list = normalize_ingredients(ingredients_raw)
    description = detail_result.get("description")
    fssai_license = detail_result.get("fssai_license")
    ocr_text = detail_result.get("ocr_text")
    barcode = detail_result.get("barcode")
    product_link = product.get("product_link")
    listing_urls = [product_link] if product_link else []
    additional_parts = []
    if description:
        additional_parts.append(description)
    if ingredients_raw:
        additional_parts.append(f"Ingredients: {ingredients_raw}")
    if fssai_license:
        additional_parts.append(f"FSSAI License: {fssai_license}")
    if ocr_text:
        additional_parts.append(f"OCR Text:\n{ocr_text}")
    if barcode:
        additional_parts.append(f"Barcode: {barcode}")
    additional_text = "\n".join(additional_parts) if additional_parts else None

    if not nutrition:
        return None
    return {
        "listingName": product.get("listing_name"),
        "detailName": detail_result.get("detail_name"),
        "listingImage": product.get("listing_image"),
        "productLink": product.get("product_link"),
        "listingUrls": listing_urls,
        "heroImages": detail_result.get("hero_images") or [],
        "nutrition": nutrition,
        "barcode": barcode,
        "ingredients": ingredients_list,
        "description": description,
        "fssaiId": fssai_license,
        "servingSize": None,
        "servingsPerPackage": None,
        "storageInfo": None,
        "expiryInfo": None,
        "additionalText": additional_text,
        "use_llm": True,
    }


def candidate_product_names(product: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for name in (
        product.get("listing_name"),
        product.get("detail", {}).get("detail_name"),
    ):
        normalized = normalize_name(name if isinstance(name, str) else None)
        if normalized:
            names.append(normalized)
    return names


def record_uploaded_product_names(
    product: Dict[str, Any], registry: Set[str]
) -> None:
    for name in candidate_product_names(product):
        registry.add(name)


def product_exists_in_registry(
    product: Dict[str, Any], registry: Set[str]
) -> bool:
    candidates = candidate_product_names(product)
    if not candidates:
        return False
    return any(name in registry for name in candidates)


def upload_product_to_nutrisnap(
    product: Dict[str, Any],
    session: requests.Session,
    registry: Set[str],
    dry_run: bool = False,
) -> bool:
    if product_exists_in_registry(product, registry):
        print("  Skipping upload; product already exists in NutriSnap.")
        return False

    payload = build_upload_payload(product)
    if payload is None:
        return False
    if dry_run:
        print("  Dry-run enabled; skipping upload to NutriSnap.")
        return False
    try:
        response = session.post(
            NUTRISNAP_UPLOAD_URL, json=payload, timeout=200  # seconds
        )
        response.raise_for_status()
        print(
            f"  Uploaded nutrition data to NutriSnap (status {response.status_code})."
        )
        record_uploaded_product_names(product, registry)
        return True
    except requests.RequestException as exc:  # pragma: no cover
        response_text = ""
        if getattr(exc, "response", None) is not None:
            response_text = getattr(exc.response, "text", "") or ""
        print(f"  Failed to upload product to NutriSnap: {exc}", file=sys.stderr)
        if response_text:
            snippet = response_text.strip().replace("\n", " ")
            print(f"  Response snippet: {snippet[:300]}", file=sys.stderr)
        return False


def run_pipeline(args: argparse.Namespace) -> Path:
    listing_target, products, diagnostics = scrape_listing(
        args.list_url, args.headless, args.listing_timeout
    )
    if not products:
        raise RuntimeError(
            "Listing scrape returned no products. "
            "Try adjusting --listing-timeout or updating the selectors."
        )

    if args.max_products is not None:
        products = products[: max(args.max_products, 0)]

    combined_results: List[Dict[str, Any]] = []
    ocr_creds_path = args.ocr_creds.expanduser().resolve()
    with requests.Session() as session:
        existing_names = fetch_existing_product_names(session)
        print(
            f"Fetched {len(existing_names)} existing NutriSnap product names for deduplication."
        )
        for idx, (name, image, link) in enumerate(products, start=1):
            print(f"[{idx}/{len(products)}] Listing product: {name}")
            detail_data: Dict[str, Any]
            if link:
                print(f"  Fetching detail page: {link}")
                detail_data = scrape_product_detail(
                    link, args.headless, args.product_timeout
                )
                if detail_data["error"]:
                    print(
                        f"  Detail scrape failed: {detail_data['error']}",
                        file=sys.stderr,
                    )
            else:
                detail_data = {
                    "detail_name": None,
                    "hero_images": [],
                    "nutrition": [],
                    "ingredients": None,
                    "description": None,
                    "fssai_license": None,
                    "error": "No product link from listing scrape.",
                }
                print(
                    "  No product link available; skipping detail scrape.",
                    file=sys.stderr,
                )

            product_record = {
                "listing_name": name,
                "listing_image": image,
                "product_link": link,
                "detail": detail_data,
            }
            if product_exists_in_registry(product_record, existing_names):
                print("  Skipping OCR and upload; product already exists in NutriSnap.")
                combined_results.append(product_record)
                continue

            image_urls: List[str] = []
            if image:
                image_urls.append(image)
            image_urls.extend(detail_data.get("hero_images") or [])
            barcode_value = run_barcode_on_images(image_urls)
            ocr_texts = run_ocr_on_images(image_urls, ocr_creds_path)
            if ocr_texts:
                detail_data["ocr_text"] = "\n\n".join(ocr_texts)
            if barcode_value:
                detail_data["barcode"] = barcode_value

            combined_results.append(product_record)
            upload_product_to_nutrisnap(
                product_record, session, existing_names, args.dry_run
            )

    output_dir = args.output_dir.expanduser().resolve()
    report_path = write_json_report(output_dir, listing_target, diagnostics, combined_results)
    return report_path


def main() -> int:
    args = parse_args()
    try:
        report_path = run_pipeline(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while generating batch report: {exc}", file=sys.stderr)
        return 1

    print(f"\nCombined report saved to: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


