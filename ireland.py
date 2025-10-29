import requests
from bs4 import BeautifulSoup
import re
from utils import process_pdf, normalize_date
import logging

def scrape_ireland(country_cfg):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    base_url = country_cfg["url"]
    logging.info(f"Scraping Ireland ({base_url}) ...")
    
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(base_url, headers=headers, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch Ireland page: {e}")
        return []
    
    soup = BeautifulSoup(res.text, "lxml")
    pdf_links = [a for a in soup.find_all("a", href=True) if ".pdf" in a["href"].lower()]
    data_list = []
    
    if not pdf_links:
        logging.warning("No PDF links found!")
        return data_list
    
    for link in pdf_links:
        try:
            href = link.get("href")
            if href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = "https://www.hpra.ie" + href
            
            title = link.get_text(strip=True) or "Untitled"
            
            # Find effective date near link
            effective_date = None
            parent_text = link.find_parent().get_text(" ", strip=True)
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", parent_text)
            if m:
                effective_date = m.group(1)
            
            # Assume publish_date is the same as effective_date if not specified
            publish_date = effective_date
            modified_date = None  # No modified date provided
            
            metadata, file_path = process_pdf(
                link,
                country_cfg,
                topic="Guidelines",
                page_url=base_url,
                effective_date=effective_date,
                publish_date=publish_date,
                modified_date=modified_date
            )
            
            if metadata and file_path:
                data_list.append((metadata, file_path))
                logging.info(f"Processed Ireland Doc: {title} | {pdf_url} | Effective Date: {effective_date}")
            else:
                logging.warning(f"Failed to process: {title}")
        
        except Exception as e:
            logging.error(f"Error processing link: {e}")
            continue
    
    return data_list