-----------------------------------------------------------------------
main command : 
python blinkit_batch_scraper.py --list-url https://blinkit.com/cn/chips-crisps/cid/1237/940(this is the listing url) --ocr-creds .\nutrisnap-82709-firebase-adminsdk-fbsvc-48a3722abf.json -l mumbai
-----------------------------------------------------------------------
example : 
python blinkit_batch_scraper.py --list-url https://blinkit.com/cn/null/cid/15/954 --ocr-creds .\nutrisnap-82709-firebase-adminsdk-fbsvc-48a3722abf.json      




Blinkit scraping helpers
========================

Quick reference for running the three helper scripts in this repo.

Prereqs
- Python 3.10+ recommended.
- Install deps: `pip install -r requirements.txt` (or install `selenium`, `beautifulsoup4`, `requests`, `google-cloud-vision`, `webdriver-manager`).

blinkit_products_scraper.py
- Purpose: scrape a Blinkit listing page (or saved `blinkit_page.html`) to collect product names, hero images, and links.
- Basic run: `python blinkit_products_scraper.py`
- Target a specific page: `python blinkit_products_scraper.py --url https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953`
- Use a saved snapshot: `python blinkit_products_scraper.py --url blinkit_page.html`
- Headless Chrome: `python blinkit_products_scraper.py --headless`
- Adjust wait time: `python blinkit_products_scraper.py --timeout 30`

blinkit_batch_scraper.py
- Purpose: end-to-end pipeline — scrape listing, scrape each product detail page, run OCR on images, write a combined JSON report, optionally upload nutrition data to NutriSnap.
- Basic run (writes to `scrappedData/blinkit_combined_report_<timestamp>.json`):  
  `python blinkit_batch_scraper.py`
- Limit products: `python blinkit_batch_scraper.py --max-products 25`
- Custom listing URL and timeouts:  
  `python blinkit_batch_scraper.py --list-url https://blinkit.com/... --listing-timeout 25 --product-timeout 30`
- Headless mode: `python blinkit_batch_scraper.py --headless`
- Set delivery location (presets: delhi, mumbai, bangalore, gurugram):  
  `python blinkit_batch_scraper.py --list-url https://blinkit.com/... --location delhi`
- Custom location via raw JSON:  
  `python blinkit_batch_scraper.py --list-url https://blinkit.com/... -l '{"coords":{"lat":19.076,"lon":72.877,"locality":"Mumbai",...}}'`
- Custom output directory: `python blinkit_batch_scraper.py --output-dir reports/`
- Use specific Vision API creds for OCR: `python blinkit_batch_scraper.py --ocr-creds path/to/service_account.json`
- Skip uploads (fetch and OCR only): `python blinkit_batch_scraper.py --dry-run`


ocr_image.py
- Purpose: run Google Vision TEXT_DETECTION on a local image or image URL.
- OCR a local file: `python ocr_image.py --image path/to/file.jpg`
- OCR a remote image: `python ocr_image.py --url https://example.com/image.jpg`
- Use alternate creds: `python ocr_image.py --creds path/to/service_account.json`
- Print raw annotations JSON: `python ocr_image.py --image path/to/file.jpg --json`

Notes
- The scrapers rely on Chrome; `webdriver-manager` auto-installs chromedriver.
- Blinkit may block headless sessions; run without `--headless` if results look empty.

