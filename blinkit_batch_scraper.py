#!/usr/bin/env python3
"""
Run the listing scraper to collect Blinkit products, then visit each product page
with the single-product scraper and dump everything into one text report.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import blinkit_products_scraper as listing
import blinkit_single_product_scraper as detail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Chain the Blinkit listing scraper with the single-product scraper "
            "and write a combined text report."
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
        "--output",
        "-o",
        default="blinkit_combined_report.txt",
        help="Path to the output text file.",
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
        product_name, hero_images, nutrition = detail.extract_product_info(state)
        return {
            "detail_name": product_name,
            "hero_images": hero_images,
            "nutrition": nutrition,
            "error": None,
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "detail_name": None,
            "hero_images": [],
            "nutrition": [],
            "error": str(exc),
        }
    finally:
        if driver is not None:
            driver.quit()


def write_report(
    output_path: Path,
    listing_target: str,
    diagnostics: Dict[str, Any],
    products: List[Dict[str, Any]],
) -> None:
    lines: List[str] = []
    lines.append("Blinkit Batch Scrape Report")
    lines.append(f"Listing target: {listing_target}")
    lines.append(f"Total listing cards: {diagnostics.get('candidate_cards')}")
    lines.append(
        f"Products with names/images/links: "
        f"{diagnostics.get('cards_with_names')}/"
        f"{diagnostics.get('cards_with_images')}/"
        f"{diagnostics.get('cards_with_links')}"
    )
    lines.append(f"Products processed: {len(products)}")

    for idx, product in enumerate(products, start=1):
        lines.append("")
        lines.append(f"Product #{idx}")
        lines.append(f"Listing name : {product['listing_name']}")
        lines.append(f"Listing image: {product['listing_image']}")
        lines.append(f"Product link : {product.get('product_link') or '<not available>'}")

        detail_result = product["detail"]
        if detail_result["error"]:
            lines.append(f"Detail error : {detail_result['error']}")
            continue

        lines.append(f"Detail name  : {detail_result.get('detail_name') or '<not found>'}")

        hero_images: List[str] = detail_result.get("hero_images", [])
        if hero_images:
            lines.append("Hero images :")
            for img_idx, img in enumerate(hero_images, start=1):
                lines.append(f"  {img_idx}. {img}")
        else:
            lines.append("Hero images : <none>")

        nutrition: List[Tuple[str, str]] = detail_result.get("nutrition", [])
        if nutrition:
            lines.append("Nutrition   :")
            for label, value in nutrition:
                lines.append(f"  - {label}: {value}")
        else:
            lines.append("Nutrition   : <none>")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    for idx, (name, image, link) in enumerate(products, start=1):
        print(f"[{idx}/{len(products)}] Listing product: {name}")
        detail_data: Dict[str, Any]
        if link:
            print(f"  Fetching detail page: {link}")
            detail_data = scrape_product_detail(link, args.headless, args.product_timeout)
            if detail_data["error"]:
                print(f"  Detail scrape failed: {detail_data['error']}", file=sys.stderr)
        else:
            detail_data = {
                "detail_name": None,
                "hero_images": [],
                "nutrition": [],
                "error": "No product link from listing scrape.",
            }
            print("  No product link available; skipping detail scrape.", file=sys.stderr)

        combined_results.append(
            {
                "listing_name": name,
                "listing_image": image,
                "product_link": link,
                "detail": detail_data,
            }
        )

    output_path = Path(args.output).expanduser().resolve()
    write_report(output_path, listing_target, diagnostics, combined_results)
    return output_path


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


