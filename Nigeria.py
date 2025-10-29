import requests
from bs4 import BeautifulSoup
import re
import logging
from utils import process_pdf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_nigeria(country_cfg):
    base_url = country_cfg["url"]
    logging.info(f"Scraping Nigeria ({base_url}) ...")

    data_list = []

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        res = session.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch Nigeria page -> {e}")
        return data_list

    soup = BeautifulSoup(res.text, "lxml")
    pdf_links = soup.select("a[href$='.pdf']")

    for link in pdf_links:
        effective_date = None  # Always None - handled in database insertion

        logging.info(f"PDF: {link.get('href')} | Effective Date: {effective_date}")  # Optional: Keep for logging if needed

        metadata, file_path = process_pdf(
            link,
            country_cfg,
            topic="Guidelines",
            page_url=base_url,
            effective_date=effective_date  # Pass None - no extraction in scraper
        )
        if file_path:
            data_list.append((metadata, file_path))
    
    return data_list


# import requests
# from bs4 import BeautifulSoup
# import re
# import logging
# from utils import process_pdf
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def scrape_nigeria(country_cfg):
#     base_url = country_cfg["url"]
#     logging.info(f"Scraping Nigeria ({base_url}) ...")

#     data_list = []

#     session = requests.Session()
#     retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
#     session.mount('https://', HTTPAdapter(max_retries=retries))

#     try:
#         res = session.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#         res.raise_for_status()
#     except Exception as e:
#         logging.error(f"Failed to fetch Nigeria page -> {e}")
#         return data_list

#     soup = BeautifulSoup(res.text, "lxml")
#     pdf_links = soup.select("a[href$='.pdf']")

#     for link in pdf_links:
#         effective_date = None  

#         row = link.find_parent("tr")
#         if row:
#             text = row.get_text(" ", strip=True)
#             m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\b\w+\s+\d{4})", text)
#             if m:
#                 effective_date = m.group(1)

#         if not effective_date:
#             sibling_text = link.find_next(string=True)
#             if sibling_text:
#                 m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\b\w+\s+\d{4})", sibling_text)
#                 if m:
#                     effective_date = m.group(1)

#         logging.info(f"PDF: {link.get('href')} | Effective Date: {effective_date}")

#         metadata, file_path = process_pdf(
#             link,
#             country_cfg,
#             topic="Guidelines",
#             page_url=base_url,
#             effective_date=effective_date
#         )
#         if file_path:
#             data_list.append((metadata, file_path))
    
#     return data_list
# # Nigeria.py
# import requests
# from bs4 import BeautifulSoup
# import re
# import logging
# import os
# from datetime import datetime
# # Assuming no utils.process_pdf provided, implement download here

# def download_pdf(pdf_url, title):
#     try:
#         res = requests.get(pdf_url, timeout=30)
#         res.raise_for_status()
#         file_path = f"/tmp/{title.replace(' ', '_').replace('/', '_')}.pdf"
#         with open(file_path, 'wb') as f:
#             f.write(res.content)
#         return file_path
#     except Exception as e:
#         logging.error(f"Failed to download {pdf_url}: {e}")
#         return None

# def scrape_nigeria(country_cfg):
#     base_url = country_cfg["url"]
#     print(f"\n Scraping Nigeria ({base_url}) ...")

#     data_list = []

#     try:
#         res = requests.get(base_url, timeout=30)
#         res.raise_for_status()
#     except Exception as e:
#         print(f" Failed to fetch Nigeria page -> {e}")
#         return data_list

#     soup = BeautifulSoup(res.text, "lxml")
#     pdf_links = soup.select("a[href$='.pdf']")

#     for link in pdf_links:
#         effective_date = None  

#         # Try to find date in parent <tr>
#         row = link.find_parent("tr")
#         if row:
#             text = row.get_text(" ", strip=True)
#             m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\b\w+\s+\d{4})", text)
#             if m:
#                 effective_date = m.group(1)

#         # If not found, check sibling text
#         if not effective_date:
#             sibling_text = link.find_next(string=True)
#             if sibling_text:
#                 m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{4}|\b\w+\s+\d{4})", sibling_text)
#                 if m:
#                     effective_date = m.group(1)

#         print(f" PDF: {link.get('href')} | Effective Date: {effective_date}")

#         pdf_url = link.get('href')
#         if not pdf_url.startswith('http'):
#             pdf_url = requests.urljoin(base_url, pdf_url)
        
#         title = link.text.strip() or "Untitled"
#         description = ""  # Extract better if possible, e.g., from alt or nearby text
        
#         file_path = download_pdf(pdf_url, title)
#         if not file_path:
#             continue
        
#         metadata = {
#             'title': title,
#             'url': pdf_url,
#             'description': description,
#             'posted_date': effective_date,
#             'modified_date': None,
#             'effective_date': effective_date
#         }
        
#         data_list.append((metadata, file_path))
    
#     return data_list