import os
import re
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber
from utils import process_pdf 

# ========= LOGGING CONFIG =========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ========= SESSION CREATION =========
def create_session_with_retries():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

# ========= EXTRACT PDF SUMMARY =========
def extract_pdf_summary(pdf_path):
    description = ''
    if os.path.exists(pdf_path):
        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ''
                for page in pdf.pages[:2]:  # First 2 pages for summary
                    full_text += page.extract_text() or ''
                
                if full_text.strip():
                    description = full_text[:200].strip() + '...'
                    # Extract any dates (e.g., "Effective Date: DD/MM/YYYY")
                    date_pattern = r'(?:Date|Effective|Published|Version)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
                    matches = re.findall(date_pattern, full_text, re.IGNORECASE)
                    if matches:
                        description += f' [Extracted dates: {", ".join(matches)}]'
                else:
                    description = '[PDF appears empty or unreadable]'
        except Exception as e:
            description = f'[Error reading PDF: {str(e)}]'
    return description

# ========= MAIN SCRAPER FUNCTION =========
def scrape_south_africa(config):
    base_url = 'https://www.sahpra.org.za/medical-devices-and-in-vitro-diagnostics-guidelines/'
    save_dir = config.get('save_dir', r'C:\Users\HP\AppData\Local\Temp')
    timeout = config.get('timeout', 10)
    country = config.get('country', 'South Africa')
    agency_id = config.get('agency_id', 'SAHPRA')

    data_list = []

    try:
        session = create_session_with_retries()
        response = session.get(base_url, timeout=timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        if not table:
            logging.error(f"No table found on {base_url}")
            return data_list

        rows = table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 5:
                continue

            # Extract data
            doc_num = cols[0].text.strip()
            title_link = cols[1].find('a')
            if not title_link or 'href' not in title_link.attrs:
                continue

            title = title_link.text.strip() or 'Unnamed Guideline'
            pdf_url = urljoin(base_url, title_link['href'])

            # Extract date
            date_str = cols[3].text.strip()
            posted_date = None
            if date_str:
                try:
                    day, month, year = date_str.split('/')
                    full_year = f"20{year}" if len(year) == 2 else year
                    posted_date = f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"
                except ValueError:
                    posted_date = date_str

            # Use the shared process_pdf() (handles skip logic + DB check)
            metadata, file_path = process_pdf(
                link=title_link,
                country_cfg={'country': country, 'url': base_url, 'agency_id': agency_id},
                topic=title,
                page_url=base_url,
                posted_date=posted_date
            )

            if not file_path:
                continue

            # Extract summary
            description = extract_pdf_summary(file_path)
            metadata['description'] = description
            metadata['doc_num'] = doc_num

            data_list.append((metadata, file_path))

        logging.info(f"Scraping completed for {country} ({agency_id}). Total PDFs: {len(data_list)}")
        return data_list

    except Exception as e:
        logging.error(f"Error scraping {country} ({agency_id}) data: {e}")
        return []

# ========= TEST RUN =========
if __name__ == "__main__":
    config = {
        'country': 'South Africa',
        'save_dir': r'C:\Users\HP\AppData\Local\Temp',
        'timeout': 10,
        'starting_docket_id': 1000,
        'agency_id': 'SAHPRA',
    }
    data = scrape_south_africa(config)
    for metadata, file_path in data:
        print(f"Title: {metadata['title']}, Date: {metadata['posted_date']}, Description: {metadata['description'][:50]}..., File: {file_path}")

# import os
# import logging
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import hashlib

# # ========= LOGGING CONFIG =========
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # ========= SESSION CREATION =========
# def create_session_with_retries():
#     session = requests.Session()
#     retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount('https://', HTTPAdapter(max_retries=retries))
#     return session

# # ========= DOWNLOAD PDF FUNCTION =========
# def download_pdf(url, save_dir, country, agency_id, timeout=10):
#     """
#     Download a PDF with retries and save it to a unique file path.
#     Includes country and agency name in logs.
#     """
#     try:
#         # Generate a unique file name based on URL
#         file_name = hashlib.md5(url.encode()).hexdigest() + '.pdf'
#         file_path = os.path.join(save_dir, file_name)

#         # If file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists for {country} ({agency_id}), reusing: {file_path}")
#             return file_path

#         # Download with retry session
#         session = create_session_with_retries()
#         response = session.get(url, timeout=timeout)
#         response.raise_for_status()

#         # Save file
#         with open(file_path, 'wb') as f:
#             f.write(response.content)

#         logging.info(f"Downloaded PDF for {country} ({agency_id}): {file_path}")
#         return file_path

#     except Exception as e:
#         logging.error(f"Failed to download PDF from {url} for {country} ({agency_id}): {e}")
#         return None

# # ========= MAIN SCRAPER FUNCTION =========
# def scrape_south_africa(config):
#     """
#     Scrape medical device and IVD guidelines from SAHPRA website.
    
#     Args:
#         config (dict): Country-specific configuration.
    
#     Returns:
#         list: List of tuples (metadata, file_path).
#     """
#     base_url = 'https://www.sahpra.org.za/medical-devices-and-in-vitro-diagnostics-guidelines/'
#     save_dir = config.get('save_dir', r'C:\Users\HP\AppData\Local\Temp')
#     timeout = config.get('timeout', 10)
#     country = config.get('country', 'South Africa')
#     agency_id = config.get('agency_id', 'SAHPRA')

#     data_list = []

#     try:
#         session = create_session_with_retries()
#         response = session.get(base_url, timeout=timeout)
#         response.raise_for_status()

#         soup = BeautifulSoup(response.content, 'html.parser')
#         links = soup.find_all('a', href=True)

#         for link in links:
#             href = link.get('href', '')
#             if not href.lower().endswith('.pdf'):
#                 continue

#             pdf_url = urljoin(base_url, href)
#             title = link.get_text(strip=True) or 'Unnamed Guideline'

#             # Download the PDF (now includes country + agency info)
#             file_path = download_pdf(pdf_url, save_dir, country, agency_id, timeout)
#             if not file_path:
#                 continue

#             metadata = {
#                 'title': title,
#                 'url': pdf_url,
#                 'posted_date': None,
#                 'description': '',
#             }

#             data_list.append((metadata, file_path))

#         logging.info(f" Scraping completed for {country} ({agency_id}). Total PDFs: {len(data_list)}")
#         return data_list

#     except Exception as e:
#         logging.error(f"Error scraping {country} ({agency_id}) data: {e}")
#         return []

# # ========= TEST RUN =========
# if __name__ == "__main__":
#     config = {
#         'country': 'South Africa',
#         'save_dir': r'C:\Users\HP\AppData\Local\Temp',
#         'timeout': 10,
#         'starting_docket_id': 1000,
#         'agency_id': 'SAHPRA',
#     }
#     data = scrape_south_africa(config)
#     for metadata, file_path in data:
#         print(f"Title: {metadata['title']}, File: {file_path}")



# # south_africa.py
# import os
# import logging
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import hashlib
# import time

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def create_session_with_retries():
#     session = requests.Session()
#     retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount('https://', HTTPAdapter(max_retries=retries))
#     return session

# def download_pdf(url, save_dir, timeout=10):
#     """
#     Download a PDF with retries and save it to a unique file path.
#     Returns the file path or None if download fails.
#     """
#     try:
#         # Generate a unique file name based on URL
#         file_name = hashlib.md5(url.encode()).hexdigest() + '.pdf'
#         file_path = os.path.join(save_dir, file_name)
        
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, reusing: {file_path}")
#             return file_path
        
#         session = create_session_with_retries()
#         response = session.get(url, timeout=timeout)
#         response.raise_for_status()
        
#         with open(file_path, 'wb') as f:
#             f.write(response.content)
#         logging.info(f"Downloaded PDF: {file_path}")
#         return file_path
#     except Exception as e:
#         logging.error(f"Failed to download PDF from {url}: {e}")
#         return None

# def scrape_south_africa(config):
#     """
#     Scrape medical device and IVD guidelines from SAHPRA website.
    
#     Args:
#         config (dict): Country-specific configuration.
    
#     Returns:
#         list: List of tuples (metadata, file_path).
#     """
#     base_url = 'https://www.sahpra.org.za/medical-devices-and-in-vitro-diagnostics-guidelines/'
#     save_dir = config.get('save_dir', r'C:\Users\HP\AppData\Local\Temp')
#     timeout = config.get('timeout', 10)  # Increase timeout if needed
    
#     data_list = []
    
#     try:
#         # Create session with retries
#         session = create_session_with_retries()
#         response = session.get(base_url, timeout=timeout)
#         response.raise_for_status()
        
#         # Parse the page
#         soup = BeautifulSoup(response.content, 'html.parser')
#         links = soup.find_all('a', href=True)
        
#         for link in links:
#             href = link.get('href', '')
#             if not href.endswith('.pdf'):
#                 continue
                
#             pdf_url = urljoin(base_url, href)
#             title = link.get_text(strip=True) or 'Unnamed Guideline'
            
#             # Download the PDF
#             file_path = download_pdf(pdf_url, save_dir, timeout)
#             if not file_path:
#                 continue
                
#             # Extract metadata (modify based on actual website structure)
#             metadata = {
#                 'title': title,
#                 'url': pdf_url,
#                 'posted_date': None,  # Extract from page if available
#                 'description': '',    # Extract from page if available
#             }
            
#             data_list.append((metadata, file_path))
        
#         return data_list
    
#     except Exception as e:
#         logging.error(f"Error scraping South Africa data: {e}")
#         return []

# if __name__ == "__main__":
#     # Example config for testing
#     config = {
#         'save_dir': r'C:\Users\HP\AppData\Local\Temp',
#         'timeout': 10,
#         'starting_docket_id': 1000,
#         'agency_id': 'SAHPRA',
#     }
#     data = scrape_south_africa(config)
#     for metadata, file_path in data:
#         print(f"Title: {metadata['title']}, File: {file_path}")

# import requests
# from bs4 import BeautifulSoup
# from utils import process_pdf

# def scrape_south_africa(country_cfg):
#     base_url = country_cfg["url"]
#     print(f"\n Scraping South Africa ({base_url}) ...")

#     try:
#         res = requests.get(base_url, timeout=30)
#         res.raise_for_status()
#     except Exception as e:
#         print(f" Failed to fetch SA page -> {e}")
#         return

#     soup = BeautifulSoup(res.text, "lxml")
#     pdf_links = soup.select("a[href$='.pdf']")

#     for link in pdf_links:
#         process_pdf(link, country_cfg, topic="Guidelines", page_url=base_url)
