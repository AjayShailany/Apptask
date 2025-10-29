import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from utils import process_pdf

def scrape_belgium(country_cfg):
    base_url = country_cfg["url"]
    print(f"\n➡️ Scraping Belgium ({base_url}) ...")

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(base_url, headers=headers, timeout=30)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch Belgium page -> {e}")
        return

    soup = BeautifulSoup(res.text, "lxml")

    # Get all PDF links
    pdf_links = [a for a in soup.find_all("a", href=True) if ".pdf" in a["href"].lower()]

    if not pdf_links:
        print("⚠️ No PDF links found!")
        return

    for link in pdf_links:
        href = link["href"]
        if not href.startswith("http"):
            pdf_url = urljoin(base_url, href)
        else:
            pdf_url = href

        title = link.get_text(strip=True) or "Untitled"

        # Extract effective date (look for sibling/parent <p>)
        effective_date = None
        parent_text = link.find_parent().get_text(" ", strip=True)
        m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})", parent_text)
        if m:
            effective_date = m.group(1)

        # If still not found, search whole page for "Last updated"
        if not effective_date:
            page_text = soup.get_text(" ", strip=True)
            m = re.search(r"Last updated on\s*(\d{1,2}[/-]\d{1,2}[/-]\d{4})", page_text)
            if m:
                effective_date = m.group(1)

        print(f"📄 {title} | {pdf_url} | Effective Date: {effective_date}")

        # Save with utils
        process_pdf(
            link,
            country_cfg,
            topic="Guidelines",
            page_url=base_url,
            effective_date=effective_date
        )
