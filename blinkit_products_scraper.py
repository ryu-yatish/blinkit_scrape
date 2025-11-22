#!/usr/bin/env python3
"""
Scrape Blinkit product names and hero images using Selenium + BeautifulSoup.

The selectors are based on the saved sample page `blinkit_page.html`, so you can
point the script either to that snapshot (it will be opened via file://) or to
the live listing URL.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin

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
DEFAULT_TARGET = str(SNAPSHOT_PATH) if SNAPSHOT_PATH.exists() else DEFAULT_URL


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


def extract_products(html: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("#plpContainer div[role='button'][id]")
    products: List[Tuple[str, str]] = []

    for card in cards:
        name_tag = card.select_one(".tw-text-300.tw-font-semibold")
        if not name_tag:
            continue

        image_url = None
        for img in card.select("img"):
            src = (img.get("src") or "").strip()
            if "/product/" in src:
                image_url = urljoin(base_url, src)
                break

        if not image_url:
            continue

        products.append((name_tag.get_text(strip=True), image_url))

    return products


def main() -> int:
    args = parse_args()
    target = normalize_target(args.url)
    driver: webdriver.Chrome | None = None

    try:
        driver = build_driver(args.headless)
        html = fetch_page_source(driver, target, args.timeout)
        products = extract_products(html, target)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while scraping: {exc}", file=sys.stderr)
        return 1
    finally:
        if driver is not None:
            driver.quit()

    if not products:
        print("No products found. Try increasing --timeout or updating selectors.")
        return 2

    for idx, (name, image) in enumerate(products, start=1):
        print(f"{idx}. {name}")
        print(f"   Image: {image}")

    print(f"\nTotal products: {len(products)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

