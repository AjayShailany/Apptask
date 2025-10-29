from bs4 import BeautifulSoup
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils import process_pdf

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_singapore(country_cfg):
    base_url = country_cfg["url"]
    logging.info(f"Scraping Singapore ({base_url}) ...")

    data_list = []

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        res = session.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch Singapore page -> {e}")
        return data_list

    soup = BeautifulSoup(res.text, "lxml")
    sections = soup.select("h2") or []
    
    for sec in sections:
        outer_topic = sec.get_text(strip=True)
        logging.info(f"MAIN TOPIC: {outer_topic}")

        next_block = sec.find_next_sibling()
        while next_block and next_block.name not in ["h2", "h1"]:
            sub_head = next_block.select_one("h3, strong")
            sub_topic = sub_head.get_text(strip=True) if sub_head else outer_topic
            if sub_head:
                logging.info(f"   SUB TOPIC: {sub_topic}")

            links = next_block.select("a")
            for link in links:
                href = link.get("href") or link.get("data-file") or link.get("data-href")
                if not href:
                    continue
                topic_full = f"{outer_topic} - {sub_topic}" if sub_topic else outer_topic
                metadata, file_path = process_pdf(link, country_cfg, topic_full, country_cfg["url"])
                if file_path:
                    data_list.append((metadata, file_path))

            next_block = next_block.find_next_sibling()

    return data_list