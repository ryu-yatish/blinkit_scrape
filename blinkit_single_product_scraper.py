#!/usr/bin/env python3
"""
Scrape a single Blinkit product page for its title and hero images.

Blinkit renders product details on the client and exposes everything through
`window.grofers.PRELOADED_STATE`. This script drives Chrome via Selenium,
waits for that object to become available, and then pulls the product title and
all carousel images out of the state tree.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

DEFAULT_PRODUCT_URL = (
    "https://blinkit.com/prn/suchalis-artisan-bakehouse-multigrain-sandwich-bread/prid/460625"
)
DEFAULT_TIMEOUT = 20
NUTRITION_KEYWORDS = (
    "calorie",
    "energy",
    "protein",
    "carb",
    "sugar",
    "fat",
    "fiber",
    "cholesterol",
    "sodium",
    "potassium",
    "iron",
    "zinc",
    "vitamin",
    "mineral",
    "omega",
    "phosphorus",
)
TEXT_SECTION_KEYWORDS = {
    "ingredients": ("ingredient",),
    "description": ("description", "about", "product description"),
    "fssai_license": ("fssai",),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape a Blinkit product page for its name and hero images."
    )
    parser.add_argument(
        "--url",
        "-u",
        default=DEFAULT_PRODUCT_URL,
        help="Target Blinkit product URL (defaults to the Yoga Bar oats page).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help=(
            "Run Chrome in headless mode. Blinkit occasionally blocks headless "
            "sessions, so prefer the default (visible) mode if you encounter "
            "403/block pages."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Number of seconds to wait for Blinkit to expose PRELOADED_STATE.",
    )
    return parser.parse_args()


def build_driver(headless: bool) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    # Force a portrait-like viewport because Blinkit shows additional tabs
    # (such as "Nutritional Information") only on narrow screens.
    options.add_argument("--window-size=900,1600")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                """
            },
        )
    except WebDriverException:
        # Not fatal; this is purely to reduce headless detection.
        pass
    return driver


def is_block_page(html: str) -> bool:
    normalized = html.lower()
    return "security@blinkit.com" in normalized or "blocked you" in normalized


def wait_for_preloaded_state(
    driver: webdriver.Chrome, target: str, timeout: int
) -> Dict[str, Any]:
    driver.get(target)
    wait = WebDriverWait(driver, timeout)

    try:
        state = wait.until(
            lambda drv: drv.execute_script(
                "return (window.grofers && window.grofers.PRELOADED_STATE) || null;"
            )
        )
    except TimeoutException as exc:
        html = driver.page_source
        if is_block_page(html):
            raise RuntimeError(
                "Blinkit served a block page (security@blinkit.com). "
                "Retry without --headless or after rotating your network."
            ) from exc
        raise RuntimeError(
            "Timed out waiting for window.grofers.PRELOADED_STATE to appear."
        ) from exc

    if not isinstance(state, dict):
        raise RuntimeError("PRELOADED_STATE is not a dictionary.")
    return state


def coerce_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith("//"):
        return f"https:{url}"
    return url


def iter_media_lists(data: Dict[str, Any]) -> Iterable[List[Any]]:
    for key in ("itemList", "item_list", "horizontal_item_list"):
        value = data.get(key)
        if isinstance(value, list):
            yield value


def extract_image_from_item(item: Any) -> Optional[str]:
    if not isinstance(item, dict):
        return None
    data = item.get("data", item)
    if not isinstance(data, dict):
        return None
    media = data.get("media_content")
    if not isinstance(media, dict):
        return None
    if media.get("media_type") != "image":
        return None
    image = media.get("image")
    if not isinstance(image, dict):
        return None
    return coerce_url(image.get("url"))


def normalize_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        return normalize_text(value.get("text"))
    return None


def extract_nutrition_from_snippets(pdp: Dict[str, Any]) -> List[Tuple[str, str]]:
    payload = (
        pdp.get("snippet_list_updater_data", {})
        .get("expand_attributes", {})
        .get("payload", {})
    )
    snippets = payload.get("snippets_to_add")
    entries: List[Tuple[str, str]] = []

    if not isinstance(snippets, list):
        return entries

    for snippet in snippets:
        if not isinstance(snippet, dict):
            continue
        data = snippet.get("data")
        if not isinstance(data, dict):
            continue
        title = normalize_text(data.get("title"))
        value = normalize_text(data.get("subtitle")) or normalize_text(
            data.get("description")
        )

        if not title or not value:
            continue

        title_lower = title.lower()
        keyword_hit = any(term in title_lower for term in NUTRITION_KEYWORDS)
        has_numbers = any(char.isdigit() for char in value)
        if not (keyword_hit and ("per" in title_lower or has_numbers)):
            continue

        entries.append((title, value))

    return entries


def extract_nutrition_from_attributes(pdp: Dict[str, Any]) -> List[Tuple[str, str]]:
    attributes = (
        pdp.get("tracking", {})
        .get("le_meta", {})
        .get("custom_data", {})
        .get("seo", {})
        .get("attributes")
    )

    entries: List[Tuple[str, str]] = []
    if not isinstance(attributes, list):
        return entries

    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        label = attribute.get("name") or attribute.get("attribute_name")
        value = attribute.get("value")
        if not isinstance(label, str) or not isinstance(value, str):
            continue
        label = label.strip()
        value = value.strip()
        if not label or not value:
            continue
        label_lower = label.lower()
        keyword_hit = any(term in label_lower for term in NUTRITION_KEYWORDS)
        has_numbers = any(char.isdigit() for char in value)
        if keyword_hit and ("per" in label_lower or has_numbers):
            entries.append((label, value))

    return entries


def extract_nutrition(pdp: Dict[str, Any]) -> List[Tuple[str, str]]:
    entries = extract_nutrition_from_snippets(pdp)
    fallback_entries = extract_nutrition_from_attributes(pdp)
    combined = entries + fallback_entries

    unique: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for label, value in combined:
        key = (label.lower(), value)
        if key in seen:
            continue
        seen.add(key)
        unique.append((label, value))
    return unique


def _collect_section_from_snippets(
    snippets: Iterable[Any],
    targets: Dict[str, Tuple[str, ...]],
    results: Dict[str, Optional[str]],
) -> None:
    for snippet in snippets:
        data = snippet.get("data") if isinstance(snippet, dict) else None
        if not isinstance(data, dict):
            continue
        title = normalize_text(data.get("title"))
        value = normalize_text(data.get("subtitle")) or normalize_text(
            data.get("description")
        )
        if not title or not value:
            continue

        title_lower = title.lower()
        for key, keywords in targets.items():
            if results.get(key):
                continue
            if any(term in title_lower for term in keywords):
                results[key] = value


def extract_text_sections(pdp: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Extract free-form text sections such as ingredients/description/FSSAI."""
    results: Dict[str, Optional[str]] = {key: None for key in TEXT_SECTION_KEYWORDS}

    snippets_to_add = (
        pdp.get("snippet_list_updater_data", {})
        .get("expand_attributes", {})
        .get("payload", {})
        .get("snippets_to_add")
    )
    if isinstance(snippets_to_add, list):
        _collect_section_from_snippets(snippets_to_add, TEXT_SECTION_KEYWORDS, results)

    snippets = pdp.get("snippets")
    if isinstance(snippets, list):
        _collect_section_from_snippets(snippets, TEXT_SECTION_KEYWORDS, results)

    attributes = (
        pdp.get("tracking", {})
        .get("le_meta", {})
        .get("custom_data", {})
        .get("seo", {})
        .get("attributes")
    )
    if isinstance(attributes, list):
        for attribute in attributes:
            if not isinstance(attribute, dict):
                continue
            label = attribute.get("name") or attribute.get("attribute_name")
            value = attribute.get("value")

            if (value is None or not isinstance(value, str) or not value.strip()) and isinstance(
                attribute.get("value_info"), list
            ):
                for entry in attribute["value_info"]:
                    if not isinstance(entry, dict):
                        continue
                    candidate = entry.get("value")
                    if isinstance(candidate, str) and candidate.strip():
                        value = candidate
                        break

            if not isinstance(label, str) or not isinstance(value, str):
                continue

            label = label.strip()
            value = value.strip()
            if not label or not value:
                continue

            label_lower = label.lower()
            for key, keywords in TEXT_SECTION_KEYWORDS.items():
                if results.get(key):
                    continue
                if any(term in label_lower for term in keywords):
                    results[key] = value

    return results


def extract_product_info(
    state: Dict[str, Any]
) -> Tuple[
    Optional[str], List[str], List[Tuple[str, str]], Dict[str, Optional[str]]
]:
    pdp = (
        state.get("ui", {})
        .get("pdp", {})
        .get("bffPdp", {})
        .get("bffData", {})
    )

    product_name: Optional[str] = None
    image_urls: List[str] = []

    snippets = pdp.get("snippets")
    if isinstance(snippets, list):
        for snippet in snippets:
            data = snippet.get("data") if isinstance(snippet, dict) else None
            if not isinstance(data, dict):
                continue

            if not product_name:
                title = data.get("title")
                if isinstance(title, dict):
                    text = title.get("text")
                    if isinstance(text, str) and text.strip():
                        product_name = text.strip()

            for media_list in iter_media_lists(data):
                for item in media_list:
                    image_url = extract_image_from_item(item)
                    if image_url and image_url not in image_urls:
                        image_urls.append(image_url)

    if not product_name:
        fallback = (
            pdp.get("tracking", {})
            .get("le_meta", {})
            .get("custom_data", {})
            .get("seo", {})
            .get("product_name")
        )
        if isinstance(fallback, str) and fallback.strip():
            product_name = fallback.strip()

    nutrition_entries = extract_nutrition(pdp)
    text_sections = extract_text_sections(pdp)

    return product_name, image_urls, nutrition_entries, text_sections


def main() -> int:
    args = parse_args()
    driver: Optional[webdriver.Chrome] = None
    product_name: Optional[str] = None
    image_urls: List[str] = []
    nutrition_entries: List[Tuple[str, str]] = []
    text_sections: Dict[str, Optional[str]] = {}

    try:
        driver = build_driver(args.headless)
        state = wait_for_preloaded_state(driver, args.url, args.timeout)
        product_name, image_urls, nutrition_entries, text_sections = extract_product_info(
            state
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while scraping: {exc}", file=sys.stderr)
        return 1
    finally:
        if driver is not None:
            driver.quit()

    if product_name:
        print(f"Product name: {product_name}")
    else:
        print("Product name: <not found>")

    if image_urls:
        print("\nImage URLs:")
        for idx, url in enumerate(image_urls, start=1):
            print(f"{idx}. {url}")
        print(f"\nTotal images: {len(image_urls)}")
    else:
        print("\nImage URLs: <none found>")

    if nutrition_entries:
        print("\nNutritional Information:")
        for label, value in nutrition_entries:
            print(f"- {label}: {value}")
    else:
        print("\nNutritional Information: <none found>")

    if any(text_sections.values()):
        print("\nAdditional Details:")
        if text_sections.get("ingredients"):
            print(f"- Ingredients: {text_sections['ingredients']}")
        if text_sections.get("description"):
            print(f"- Description: {text_sections['description']}")
        if text_sections.get("fssai_license"):
            print(f"- FSSAI License: {text_sections['fssai_license']}")

    if not product_name or not image_urls:
        print(
            "\nSome product details were missing. You may need to refresh the selectors "
            "or retry without headless mode."
        )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

