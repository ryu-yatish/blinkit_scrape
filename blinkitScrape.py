from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get("https://blinkit.com/cn/dairy-breakfast/bread-pav/cid/14/953")

html = driver.page_source
soup = BeautifulSoup(html, "html.parser")

# Save to file instead of printing
with open("blinkit_page.html", "w", encoding="utf-8") as f:
    f.write(soup.prettify())

driver.quit()

print("Saved as blinkit_page.html")
