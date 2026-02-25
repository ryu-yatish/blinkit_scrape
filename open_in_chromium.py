#!/usr/bin/env python3
"""
Open a URL in Chromium/Chrome using Selenium (same setup as blinkit scraper).
"""

import json
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Profile options:
# 1. Your real Chrome profile (close Chrome first!)
REAL_CHROME_PROFILE = os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\User Data")
# 2. Separate persistent profile for Selenium
SELENIUM_PROFILE_DIR = os.path.join(os.path.dirname(__file__), "chrome_profile")

# Predefined Blinkit locations — add your own here
BLINKIT_LOCATIONS = {
    "delhi": {
        "coords": {
            "isDefault": False, "lat": 28.6139, "lon": 77.2090,
            "locality": "New Delhi", "id": 1, "isTopCity": True,
            "cityName": "DL-NCR", "landmark": "Connaught Place, New Delhi, Delhi 110001, India",
            "addressId": None,
        }
    },
    "mumbai": {
        "coords": {
            "isDefault": False, "lat": 19.0760, "lon": 72.8777,
            "locality": "Mumbai", "id": 2, "isTopCity": True,
            "cityName": "Mumbai", "landmark": "Andheri West, Mumbai, Maharashtra 400053, India",
            "addressId": None,
        }
    },
    "bangalore": {
        "coords": {
            "isDefault": False, "lat": 12.9716, "lon": 77.5946,
            "locality": "Bengaluru", "id": 3, "isTopCity": True,
            "cityName": "Bangalore", "landmark": "Koramangala, Bengaluru, Karnataka 560034, India",
            "addressId": None,
        }
    },
    "gurugram": {
        "coords": {
            "isDefault": False, "lat": 28.465204, "lon": 77.06159,
            "locality": "Gurugram", "id": 1849, "isTopCity": True,
            "cityName": "HR-NCR", "landmark": "B62, Pocket B, South City I, Sector 30, Gurugram, Haryana 122001, India",
            "addressId": None,
        }
    },
}


def build_driver(headless: bool = False, profile_mode: str = "selenium") -> webdriver.Chrome:
    """
    Build Chrome/Chromium driver with anti-detection options.
    
    profile_mode:
        "real"     - Use your actual Chrome profile (CLOSE CHROME FIRST!)
        "selenium" - Use separate persistent Selenium profile
        "none"     - Fresh session, no profile
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--window-size=1200,900")
    
    if profile_mode == "real":
        # Use your actual Chrome profile - MUST close Chrome first!
        if os.path.exists(REAL_CHROME_PROFILE):
            options.add_argument(f"--user-data-dir={REAL_CHROME_PROFILE}")
            print(f"Using REAL Chrome profile: {REAL_CHROME_PROFILE}")
            print("⚠️  Make sure Chrome is closed!")
        else:
            print(f"Chrome profile not found at {REAL_CHROME_PROFILE}, using fresh session")
    elif profile_mode == "selenium":
        # Use separate persistent Selenium profile
        os.makedirs(SELENIUM_PROFILE_DIR, exist_ok=True)
        options.add_argument(f"--user-data-dir={SELENIUM_PROFILE_DIR}")
        print(f"Using Selenium profile: {SELENIUM_PROFILE_DIR}")
    else:
        print("Using fresh session (no profile)")
    
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
        pass
    
    return driver


def set_blinkit_location(driver, location_key: str):
    """
    Inject a Blinkit location into localStorage.
    Accepts a key from BLINKIT_LOCATIONS or a raw JSON string.
    """
    if location_key in BLINKIT_LOCATIONS:
        loc_data = BLINKIT_LOCATIONS[location_key]
        print(f"Setting Blinkit location to: {location_key}")
    else:
        try:
            loc_data = json.loads(location_key)
            print(f"Setting Blinkit location from custom JSON")
        except json.JSONDecodeError:
            print(f"Unknown location '{location_key}'. Available: {', '.join(BLINKIT_LOCATIONS)}")
            return

    loc_json = json.dumps(loc_data)
    driver.execute_script(
        "window.localStorage.setItem('location', arguments[0]);", loc_json
    )


def open_url(url: str, headless: bool = False, keep_open: bool = True,
             profile_mode: str = "selenium", location: str = None):
    """Open URL in Chrome/Chromium via Selenium."""
    print(f"Opening {url} in Chrome...")
    driver = build_driver(headless, profile_mode)

    if location and "blinkit" in url.lower():
        # Navigate first so we have a blinkit origin for localStorage
        driver.get(url)
        set_blinkit_location(driver, location)
        driver.refresh()
    else:
        driver.get(url)
    
    if keep_open:
        print("Browser is open. Press Enter to close...")
        input()
        driver.quit()
    else:
        return driver


if __name__ == "__main__":
    import argparse
    loc_choices = list(BLINKIT_LOCATIONS.keys())
    parser = argparse.ArgumentParser(description="Open URL in Chrome via Selenium")
    parser.add_argument("url", nargs="?", help="URL to open")
    parser.add_argument(
        "--profile", "-p",
        choices=["real", "selenium", "none"],
        default="selenium",
        help="Profile mode: 'real' (your Chrome - close Chrome first!), 'selenium' (separate profile), 'none' (fresh)"
    )
    parser.add_argument(
        "--location", "-l",
        default=None,
        help=f"Blinkit location to inject. Presets: {', '.join(loc_choices)}. Or pass raw JSON."
    )
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    
    url = args.url or input("Enter URL to open: ")
    open_url(url, headless=args.headless, profile_mode=args.profile, location=args.location)
