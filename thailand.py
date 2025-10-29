import requests
from bs4 import BeautifulSoup
from utils import process_pdf, normalize_date
import logging

BASE_URL = "https://en.fda.moph.go.th"
PAGE_URL = "https://en.fda.moph.go.th/cat2-health-products/category/health-products-medical-devices?ppp=20&page=1"

def scrape_thailand(country_cfg):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Scraping Thailand Medical Devices Guidance Documents ({PAGE_URL}) ...")
    
    try:
        response = requests.get(PAGE_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch Thailand FDA page: {e}")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select("table tbody tr")
    data_list = []
    
    for row in rows:
        try:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            
            title = cols[1].get_text(strip=True)
            pdf_link_tag = row.select_one("a[href$='.pdf']")
            if not pdf_link_tag:
                continue
            
            file_url = pdf_link_tag["href"]
            if not file_url.startswith("http"):
                file_url = BASE_URL + file_url
            
            # Extract date from row (if available, assuming date might be in a column)
            date_text = cols[0].get_text(strip=True) if len(cols) > 0 else None  # Adjust index if date is in another column
            effective_date = normalize_date(date_text)
            publish_date = effective_date  # Use effective_date as publish_date if no separate publish date
            modified_date = None  # No modified date provided in the source
            
            metadata, file_path = process_pdf(
                pdf_link_tag,
                country_cfg,
                topic="Medical Devices Guidelines",
                page_url=PAGE_URL,
                effective_date=effective_date,
                publish_date=publish_date,
                modified_date=modified_date
            )
            
            if metadata and file_path:
                data_list.append((metadata, file_path))
                logging.info(f"Processed Thailand Doc: {title} | {file_url}")
            else:
                logging.warning(f"Failed to process: {title}")
        
        except Exception as e:
            logging.error(f"Error processing row: {e}")
            continue
    
    return data_list