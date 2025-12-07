#!/usr/bin/env python3
"""
Scrape Blinkit product names, hero images, and product links using Selenium + BeautifulSoup.

The selectors are based on the saved sample page `blinkit_page.html`, so you can
point the script either to that snapshot (it will be opened via file://) or to
the live listing URL.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

DEFAULT_URL = "https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953"
SNAPSHOT_PATH = (Path(__file__).resolve().parent / "blinkit_page.html").resolve()
DEFAULT_TARGET = DEFAULT_URL
BLINKIT_ORIGIN = "https://blinkit.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Blinkit product names and image URLs."
    )
    parser.add_argument(
        "--url",
        "-u",
        default=DEFAULT_TARGET,
        help=(
            "Target page or local HTML snapshot. By default the script uses "
            "blinkit_page.html if it lives next to this script, otherwise it "
            "falls back to the live Blinkit bread listing."
        ),
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Seconds to wait for the product grid to appear (default: 20).",
    )
    return parser.parse_args()


def normalize_target(target: str) -> str:
    """Return a URL, converting existing filesystem paths to file:// URIs."""
    path = Path(target)
    if path.exists():
        return path.resolve().as_uri()
    return target


def derive_origin(target: str) -> str:
    """Return an https origin to build Blinkit links from."""
    parsed = urlparse(target)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return BLINKIT_ORIGIN


def slugify_product_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_only = re.sub(r"[’']", "", ascii_only)
    ascii_only = ascii_only.replace("&", " and ")
    trimmed = ascii_only.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", trimmed).strip("-")
    return slug


def build_product_url(product_id: str, product_name: str, origin: str) -> Optional[str]:
    if not product_id:
        return None
    slug = slugify_product_name(product_name) if product_name else ""
    if slug:
        path = f"/prn/{slug}/prid/{product_id}"
    else:
        path = f"/prn/prid/{product_id}"
    return urljoin(origin, path.lstrip("/"))


def build_driver(headless: bool) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def fetch_page_source(driver: webdriver.Chrome, target: str, timeout: int) -> str:
    driver.get(target)
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#plpContainer"))
    )
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    return driver.page_source


def extract_products(
    html: str, base_url: str
) -> Tuple[List[Tuple[str, str, Optional[str]]], Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    normalized_html = html.lower()
    container = soup.select_one("#plpContainer")
    cards = container.select("div[role='button'][id]") if container else []

    diagnostics: Dict[str, Any] = {
        "plp_container_found": container is not None,
        "candidate_cards": len(cards),
        "cards_with_names": 0,
        "cards_with_images": 0,
        "cards_with_links": 0,
        "first_card_html": None,
        "block_page_detected": "security@blinkit.com" in normalized_html
        or "blocked you" in normalized_html,
    }

    products: List[Tuple[str, str, Optional[str]]] = []
    link_origin = derive_origin(base_url)

    for card in cards:
        if diagnostics["first_card_html"] is None:
            diagnostics["first_card_html"] = card.prettify()

        name_tag = card.select_one(".tw-text-300.tw-font-semibold")
        if not name_tag:
            continue
        diagnostics["cards_with_names"] += 1

        product_name = name_tag.get_text(strip=True)
        product_id = (card.get("id") or "").strip()

        image_url = None
        for img in card.select("img"):
            src = (img.get("src") or "").strip()
            if "/product/" in src:
                image_url = urljoin(base_url, src)
                break

        if not image_url:
            continue
        diagnostics["cards_with_images"] += 1

        product_url = build_product_url(product_id, product_name, link_origin)
        if product_url:
            diagnostics["cards_with_links"] += 1

        products.append((product_name, image_url, product_url))

    return products, diagnostics


def print_diagnostics(diagnostics: Dict[str, Any], target: str) -> None:
    print("\nDiagnostics:")
    print(f"  Target: {target}")
    print(f"  '#plpContainer' present: {diagnostics.get('plp_container_found')}")
    print(f"  Candidate cards: {diagnostics.get('candidate_cards')}")
    print(f"  Cards with product names: {diagnostics.get('cards_with_names')}")
    print(f"  Cards with hero images: {diagnostics.get('cards_with_images')}")
    print(f"  Cards with product links: {diagnostics.get('cards_with_links')}")
    if diagnostics.get("block_page_detected"):
        print("  Block page copy detected (security@blinkit.com).")

    snippet = diagnostics.get("first_card_html")
    if snippet:
        snippet = snippet.strip()
        max_len = 600
        if len(snippet) > max_len:
            snippet = f"{snippet[:max_len]}... [truncated]"
        print("  First matching card snippet:")
        for line in snippet.splitlines():
            print(f"    {line}")
    else:
        print("  First matching card snippet: <none>")


def main() -> int:
    args = parse_args()
    target = normalize_target(args.url)
    driver: webdriver.Chrome | None = None
    diagnostics: Dict[str, Any] = {}

    try:
        driver = build_driver(args.headless)
        html = fetch_page_source(driver, target, args.timeout)
        products, diagnostics = extract_products(html, target)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while scraping: {exc}", file=sys.stderr)
        return 1
    finally:
        if driver is not None:
            driver.quit()

    if not products:
        print("No products found. Try increasing --timeout or updating selectors.")
        print_diagnostics(diagnostics, target)
        return 2

    for idx, (name, image, link) in enumerate(products, start=1):
        print(f"{idx}. {name}")
        print(f"   Image: {image}")
        if link:
            print(f"   Link: {link}")
        else:
            print("   Link: <not found>")

    print(f"\nTotal products: {len(products)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

