import os
import requests
from bs4 import BeautifulSoup
from utils import process_pdf, normalize_date
import logging

def scrape_canada(country_cfg):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    base_url = country_cfg["url"]
    logging.info(f"Scraping Canada Medical Devices Guidance Documents ({base_url}) ...")
    
    try:
        res = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch Canada page: {e}")
        return []
    
    soup = BeautifulSoup(res.text, "lxml")
    links = soup.select("a[href]")
    data_list = []
    
    if not links:
        logging.warning("No links found on this page!")
        return data_list
    
    for link in links:
        try:
            href = link.get("href")
            if not href:
                continue
            
            # Normalize absolute URL
            if href.startswith("/"):
                href = "https://www.canada.ca" + href
            
            text = link.get_text(strip=True) or href
            text_lower = text.lower()
            href_lower = href.lower()
            
            # Filter: Only process Medical Devices Guidance Documents
            if ("medical-device" in href_lower) or ("guidance" in text_lower):
                if href.startswith("http"):
                    # No date information provided; set to None
                    effective_date = None
                    publish_date = None
                    modified_date = None
                    
                    metadata, file_path = process_pdf(
                        link,
                        country_cfg,
                        topic="Medical Devices Guidance Documents",
                        page_url=base_url,
                        effective_date=effective_date,
                        publish_date=publish_date,
                        modified_date=modified_date
                    )
                    
                    if metadata and file_path:
                        data_list.append((metadata, file_path))
                        logging.info(f"Processed Canada Doc: {text} | {href}")
                    else:
                        logging.warning(f"Failed to process: {text}")
                else:
                    logging.info(f"Skipped non-HTTP link: {text} -> {href}")
            else:
                logging.info(f"Skipped non-relevant link: {text} -> {href}")
        
        except Exception as e:
            logging.error(f"Error processing link: {e}")
            continue
    
    return data_list