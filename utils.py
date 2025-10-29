import os
import re
from dotenv import load_dotenv
import requests
import logging
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime, date
import dateutil.parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from fpdf import FPDF
from __init__ import ldb

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ========================= DATABASE CONNECTION =========================
def get_db_connection_str():
    """Return the database connection string using LeximGPTDb."""
    conn_str = ldb.get_connection_str()
    logging.info(f"Database connection string: {conn_str}")
    return conn_str


# ========================= FILENAME CLEANUP =========================
def sanitize_filename(filename):
    """Sanitize filename for Windows compatibility."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    filename = re.sub(r'_+', '_', filename).strip('_')
    return filename[:200]


# ========================= DATE HANDLING =========================
def normalize_date(date_input):
    """Normalize various date formats to YYYY-MM-DD string."""
    if not date_input:
        return None
    try:
        if isinstance(date_input, (datetime, date)):
            return date_input.strftime('%Y-%m-%d')
        elif isinstance(date_input, str):
            parsed = dateutil.parser.parse(date_input)
            return parsed.strftime('%Y-%m-%d')
        else:
            logging.warning(f"Unknown date type: {type(date_input)} for {date_input}")
            return None
    except Exception as e:
        logging.error(f"Failed to parse date {date_input}: {e}")
        return None


# ========================= HTML TO PDF =========================
def html_to_pdf(url, output_path):
    """Convert HTML to PDF using FPDF."""
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        text_elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'li'])
        text_content = '\n'.join(element.get_text(strip=True) for element in text_elements if element.get_text(strip=True))

        pdf = FPDF()
        pdf.add_page()
        pdf.set_font('Arial', size=12)

        for line in text_content.split('\n'):
            line = line.encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 10, line)

        pdf.output(output_path)
        logging.info(f"Converted HTML to PDF: {output_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
        return False


# ========================= CHECK IN DATABASE =========================
def is_already_downloaded(url, db):
    """Check if a document with this URL already exists in the database."""
    try:
        query = "SELECT COUNT(*) AS cnt FROM documents WHERE url = %s"
        result = db.run_query(query, (url,))
        return result[0]['cnt'] > 0
    except Exception as e:
        logging.error(f"DB check failed for {url}: {e}")
        return False


# ========================= MAIN PROCESS FUNCTION =========================
def process_pdf(link, country_cfg, topic, page_url,
                effective_date=None, modified_date=None,
                publish_date=None, posted_date=None):
    """
    Process a document link and return metadata + file path.
    Adds `public_date` (from posted_date or publish_date).
    Skips re-download if file already exists or URL is in DB.
    """
    try:
        url = link.get('href')
        if not url:
            logging.error("No href found in link.")
            return None, None

        title = link.text.strip() or topic or "Untitled"
        title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]

        if not url.startswith('http'):
            url = urljoin(page_url, url)

        # 🔹 Normalize dates
        effective_date = normalize_date(effective_date)
        modified_date = normalize_date(modified_date)
        publish_date = normalize_date(publish_date)
        public_date = normalize_date(posted_date) or publish_date

        metadata = {
            'title': title,
            'url': url,
            'description': f"{topic} document from {country_cfg.get('url', '')}",
            'publish_date': publish_date,
            'modified_date': modified_date,
            'effective_date': effective_date,
            'public_date': public_date,
            'country': country_cfg.get('country', 'Unknown')
        }

        # 🔹 Prepare file name
        parsed_url = urlparse(url)
        base_filename = unquote(os.path.basename(parsed_url.path)) or (title + '.pdf')
        _, ext = os.path.splitext(base_filename)
        if not ext:
            ext = '.pdf'
        clean_base_filename = sanitize_filename(base_filename)

        # 🔹 Ensure temp folder exists
        temp_dir = os.environ.get('TEMP', 'C:\\Temp')
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.join(temp_dir, clean_base_filename)

        # 🔹 Check in DB before downloading
        if is_already_downloaded(url, ldb):
            logging.info(f"Already exists in database, skipping: {url}")
            return metadata, file_path

        # 🔹 Check local file system
        if os.path.exists(file_path):
            logging.info(f"File already exists locally, skipping: {file_path}")
            return metadata, file_path

        # 🔹 Download new file
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        is_html = ext.lower() in ['.html', '.htm']
        if is_html:
            if not html_to_pdf(url, file_path):
                return metadata, None
        else:
            response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logging.info(f"Downloaded file: {file_path}")

        return metadata, file_path

    except Exception as e:
        logging.error(f"Error processing document {url if 'url' in locals() else 'unknown'}: {e}")
        return None, None


# import os
# import re
# from dotenv import load_dotenv
# import requests
# import logging
# from urllib.parse import urljoin, urlparse, unquote
# from datetime import datetime, date
# import dateutil.parser
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from bs4 import BeautifulSoup
# from fpdf import FPDF
# from __init__ import ldb  

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# # ========================= DATABASE CONNECTION =========================
# def get_db_connection_str():
#     """Return the database connection string using LeximGPTDb."""
#     conn_str = ldb.get_connection_str()
#     logging.info(f"Database connection string: {conn_str}")
#     return conn_str


# # ========================= FILENAME CLEANUP =========================
# def sanitize_filename(filename):
#     """Sanitize filename for Windows compatibility."""
#     invalid_chars = '<>:"/\\|?*'
#     for char in invalid_chars:
#         filename = filename.replace(char, '_')
#     filename = re.sub(r'_+', '_', filename).strip('_')
#     return filename[:200]


# # ========================= DATE HANDLING =========================
# def normalize_date(date_input):
#     """Normalize various date formats to YYYY-MM-DD string."""
#     if not date_input:
#         return None
#     try:
#         if isinstance(date_input, (datetime, date)):
#             return date_input.strftime('%Y-%m-%d')
#         elif isinstance(date_input, str):
#             parsed = dateutil.parser.parse(date_input)
#             return parsed.strftime('%Y-%m-%d')
#         else:
#             logging.warning(f"Unknown date type: {type(date_input)} for {date_input}")
#             return None
#     except Exception as e:
#         logging.error(f"Failed to parse date {date_input}: {e}")
#         return None


# # ========================= HTML TO PDF =========================
# def html_to_pdf(url, output_path):
#     """Convert HTML to PDF using FPDF."""
#     try:
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
#         response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#         response.raise_for_status()

#         soup = BeautifulSoup(response.text, 'html.parser')
#         text_elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'li'])
#         text_content = '\n'.join(element.get_text(strip=True) for element in text_elements if element.get_text(strip=True))

#         pdf = FPDF()
#         pdf.add_page()
#         pdf.set_font('Arial', size=12)

#         for line in text_content.split('\n'):
#             line = line.encode('latin-1', 'replace').decode('latin-1')
#             pdf.multi_cell(0, 10, line)

#         pdf.output(output_path)
#         logging.info(f"Converted HTML to PDF: {output_path}")
#         return True
#     except Exception as e:
#         logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#         return False


# # ========================= MAIN PROCESS FUNCTION =========================
# def process_pdf(link, country_cfg, topic, page_url,
#                 effective_date=None, modified_date=None,
#                 publish_date=None, posted_date=None):
#     """
#     Process a document link and return metadata + file path.
#     Adds `public_date` from posted_date.
#     """
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link.")
#             return None, None

#         title = link.text.strip() or topic or "Untitled"
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]

#         if not url.startswith('http'):
#             url = urljoin(page_url, url)

#         # Log incoming date fields
#         logging.info(f"Input Dates -> effective: {effective_date}, modified: {modified_date}, publish: {publish_date}, posted: {posted_date}")

#         # Normalize all dates
#         effective_date = normalize_date(effective_date)
#         modified_date = normalize_date(modified_date)
#         publish_date = normalize_date(publish_date)
#         public_date = normalize_date(posted_date) or publish_date  # Add posted_date as public_date fallback

#         # Log normalized dates
#         logging.info(f"Normalized -> effective: {effective_date}, modified: {modified_date}, publish: {publish_date}, public: {public_date}")

#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg.get('url', '')}",
#             'publish_date': publish_date,
#             'modified_date': modified_date,
#             'effective_date': effective_date,
#             'public_date': public_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }

#         # File handling
#         parsed_url = urlparse(url)
#         base_filename = unquote(os.path.basename(parsed_url.path)) or (title + '.pdf')
#         _, ext = os.path.splitext(base_filename)
#         if not ext:
#             ext = '.pdf'

#         clean_base_filename = sanitize_filename(base_filename)
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, clean_base_filename)

#         if os.path.exists(file_path):
#             logging.info(f"File already exists: {file_path}")
#             return metadata, file_path

#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))

#         is_html = ext.lower() in ['.html', '.htm']
#         if is_html:
#             if not html_to_pdf(url, file_path):
#                 return metadata, None
#         else:
#             response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#             response.raise_for_status()
#             with open(file_path, 'wb') as f:
#                 f.write(response.content)
#             logging.info(f"Downloaded file: {file_path}")

#         return metadata, file_path

#     except Exception as e:
#         logging.error(f"Error processing document {url if 'url' in locals() else 'unknown'}: {e}")
#         return None, None


# import os
# import re
# from dotenv import load_dotenv
# import requests
# import logging
# from urllib.parse import urljoin, urlparse, unquote
# from datetime import datetime
# import dateutil.parser
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from bs4 import BeautifulSoup
# from fpdf import FPDF
# from __init__ import ldb  

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_db_connection_str():
#     """Return the database connection string using LeximGPTDb."""
#     conn_str = ldb.get_connection_str()
#     logging.info(f"Database connection string: {conn_str}")
#     return conn_str

# def sanitize_filename(filename):
#     """Sanitize filename for Windows compatibility: remove/replace invalid characters."""
#     invalid_chars = '<>:"/\\|?*'
#     for char in invalid_chars:
#         filename = filename.replace(char, '_')
#     # Replace multiple underscores and trim
#     filename = re.sub(r'_+', '_', filename).strip('_')
#     return filename[:200]  # Limit length to avoid path issues

# def normalize_date(date_input):
#     """Normalize a date input to YYYY-MM-DD format. Handles strings, datetime objects, or None."""
#     if not date_input:
#         return None
#     try:
#         if isinstance(date_input, (datetime, datetime.date)):
#             return date_input.strftime('%Y-%m-%d')
#         elif isinstance(date_input, str):
#             parsed = dateutil.parser.parse(date_input)
#             return parsed.strftime('%Y-%m-%d')
#         else:
#             logging.error(f"Invalid date input type: {type(date_input)} for {date_input}")
#             return None
#     except Exception as e:
#         logging.error(f"Failed to parse date {date_input}: {e}")
#         return None

# def html_to_pdf(url, output_path):
#     """Convert HTML content from a URL to PDF using FPDF."""
#     try:
#         # Fetch HTML content
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
#         response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#         response.raise_for_status()
        
#         # Parse HTML with BeautifulSoup
#         soup = BeautifulSoup(response.text, 'html.parser')
        
#         # Extract text content (e.g., from <p>, <h1>, <h2>, etc.)
#         text_elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'])
#         text_content = '\n'.join(element.get_text(strip=True) for element in text_elements if element.get_text(strip=True))
        
#         # Initialize FPDF
#         pdf = FPDF()
#         pdf.add_page()
#         pdf.set_font('Arial', size=12)
        
#         # Add text to PDF, handling line breaks
#         for line in text_content.split('\n'):
#             # Encode text to handle special characters
#             line = line.encode('latin-1', 'replace').decode('latin-1')
#             pdf.multi_cell(0, 10, line)
        
#         # Save PDF
#         pdf.output(output_path)
#         logging.info(f"Converted HTML to PDF using FPDF: {output_path}")
#         return True
#     except Exception as e:
#         logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#         return False

# def process_pdf(link, country_cfg, topic, page_url, effective_date=None, modified_date=None, publish_date=None):
#     """Process a document link: download PDF or convert HTML to PDF, return metadata and file path.
#     Skip download if the file already exists."""
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link")
#             return None, None
        
#         # Use topic as fallback title if link.text is empty
#         title = link.text.strip() or topic or "Untitled"
        
#         # Clean title aggressively
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]
        
#         if not url.startswith('http'):
#             url = urljoin(page_url, url)
        
#         # Log input dates
#         logging.info(f"Input dates - effective_date: {effective_date}, modified_date: {modified_date}, publish_date: {publish_date}")
        
#         # Normalize dates
#         effective_date = normalize_date(effective_date)
#         modified_date = normalize_date(modified_date)
#         publish_date = normalize_date(publish_date)
        
#         # Log normalized dates
#         logging.info(f"Normalized dates - effective_date: {effective_date}, modified_date: {modified_date}, publish_date: {publish_date}")
        
#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg['url']}",
#             'publish_date': publish_date,  # Fixed from posted_date to match database
#             'modified_date': modified_date,
#             'effective_date': effective_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }
        
#         # Extract clean base filename from URL (strip query params)
#         parsed_url = urlparse(url)
#         base_filename = os.path.basename(parsed_url.path)
#         if not base_filename:
#             base_filename = title + '.pdf'  # Fallback
#         else:
#             # Unquote and clean the base filename
#             base_filename = unquote(base_filename)
#             # Get extension from base filename, fallback to .pdf
#             _, ext = os.path.splitext(base_filename)
#             if not ext:
#                 ext = '.pdf'
        
#         # Sanitize the base filename for Windows
#         clean_base_filename = sanitize_filename(base_filename)
        
#         is_html = ext.lower() in ['.html', '.htm']
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, clean_base_filename)
        
#         # Check if file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, skipping download: {file_path}")
#             return metadata, file_path
        
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         # Log the actual file type based on extension
#         file_type = 'PDF' if ext.lower() == '.pdf' else ext.upper().lstrip('.')
#         logging.info(f"Downloading {file_type}: {url}")
        
#         if is_html:
#             success = html_to_pdf(url, file_path)
#             if not success:
#                 return metadata, None
#         else:
#             try:
#                 response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#                 response.raise_for_status()
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#                 logging.info(f"Downloaded {file_type}: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to download {file_type} from {url}: {e}")
#                 return metadata, None
        
#         return metadata, file_path
    
#     except Exception as e:
#         logging.error(f"Error processing document {url if 'url' in locals() else 'unknown'}: {e}")
#         return None, None


# import os
# import re
# from dotenv import load_dotenv
# import requests
# import logging
# from urllib.parse import urljoin, urlparse, unquote
# from datetime import datetime
# import dateutil.parser
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from __init__ import ldb  # Import LeximGPTDb instance

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_db_connection_str():
#     """Return the database connection string using LeximGPTDb."""
#     conn_str = ldb.get_connection_str()
#     logging.info(f"Database connection string: {conn_str}")
#     return conn_str

# def sanitize_filename(filename):
#     """Sanitize filename for Windows compatibility: remove/replace invalid characters."""
#     invalid_chars = '<>:"/\\|?*'
#     for char in invalid_chars:
#         filename = filename.replace(char, '_')
#     # Replace multiple underscores and trim
#     filename = re.sub(r'_+', '_', filename).strip('_')
#     return filename[:200]  # Limit length to avoid path issues

# def normalize_date(date_input):
#     """Normalize a date input to YYYY-MM-DD format. Handles strings, datetime objects, or None."""
#     if not date_input:
#         return None
#     try:
#         if isinstance(date_input, (datetime, datetime.date)):
#             return date_input.strftime('%Y-%m-%d')
#         elif isinstance(date_input, str):
#             parsed = dateutil.parser.parse(date_input)
#             return parsed.strftime('%Y-%m-%d')
#         else:
#             logging.error(f"Invalid date input type: {type(date_input)} for {date_input}")
#             return None
#     except Exception as e:
#         logging.error(f"Failed to parse date {date_input}: {e}")
#         return None

# def process_pdf(link, country_cfg, topic, page_url, effective_date=None, modified_date=None, publish_date=None):
#     """Process a document link: download PDF or convert HTML to PDF, return metadata and file path.
#     Skip download if the file already exists."""
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link")
#             return None, None
        
#         # Use topic as fallback title if link.text is empty
#         title = link.text.strip() or topic or "Untitled"
        
#         # Clean title aggressively
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]
        
#         if not url.startswith('http'):
#             url = urljoin(page_url, url)
        
#         # Log input dates
#         logging.info(f"Input dates - effective_date: {effective_date}, modified_date: {modified_date}, publish_date: {publish_date}")
        
#         # Normalize dates
#         effective_date = normalize_date(effective_date)
#         modified_date = normalize_date(modified_date)
#         publish_date = normalize_date(publish_date)
        
#         # Log normalized dates
#         logging.info(f"Normalized dates - effective_date: {effective_date}, modified_date: {modified_date}, publish_date: {publish_date}")
        
#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg['url']}",
#             'posted_date': publish_date,  # Map to publish_date
#             'modified_date': modified_date,
#             'effective_date': effective_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }
        
#         # Extract clean base filename from URL (strip query params)
#         parsed_url = urlparse(url)
#         base_filename = os.path.basename(parsed_url.path)
#         if not base_filename:
#             base_filename = title + '.pdf'  # Fallback
#         else:
#             # Unquote and clean the base filename
#             base_filename = unquote(base_filename)
#             # Get extension from base filename, fallback to .pdf
#             _, ext = os.path.splitext(base_filename)
#             if not ext:
#                 ext = '.pdf'
        
#         # Sanitize the base filename for Windows
#         clean_base_filename = sanitize_filename(base_filename)
        
#         is_html = ext.lower() in ['.html', '.htm']
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, clean_base_filename)
        
#         # Check if file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, skipping download: {file_path}")
#             return metadata, file_path
        
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         # Log the actual file type based on extension
#         file_type = 'PDF' if ext.lower() == '.pdf' else ext.upper().lstrip('.')
#         logging.info(f"Downloading {file_type}: {url}")
        
#         if is_html:
#             try:
#                 import pdfkit
#                 pdfkit.from_url(url, file_path)
#                 logging.info(f"Converted HTML to PDF: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#                 return metadata, None
#         else:
#             try:
#                 response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#                 response.raise_for_status()
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#                 logging.info(f"Downloaded {file_type}: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to download {file_type} from {url}: {e}")
#                 return metadata, None
        
#         return metadata, file_path
    
#     except Exception as e:
#         logging.error(f"Error processing document {url if 'url' in locals() else 'unknown'}: {e}")
#         return None, None

# import os
# import re
# from dotenv import load_dotenv
# import requests
# import logging
# from urllib.parse import urljoin, urlparse, unquote
# from datetime import datetime
# import dateutil.parser
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from __init__ import ldb  # Import LeximGPTDb instance

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_db_connection_str():
#     """Return the database connection string using LeximGPTDb."""
#     conn_str = ldb.get_connection_str()
#     logging.info(f"Database connection string: {conn_str}")
#     return conn_str

# def sanitize_filename(filename):
#     """Sanitize filename for Windows compatibility: remove/replace invalid characters."""
#     invalid_chars = '<>:"/\\|?*'
#     for char in invalid_chars:
#         filename = filename.replace(char, '_')
#     # Replace multiple underscores and trim
#     filename = re.sub(r'_+', '_', filename).strip('_')
#     return filename[:200]  # Limit length to avoid path issues


# def process_pdf(link, country_cfg, topic, page_url, effective_date=None, modified_date=None, publish_date=None):
#     """Process a document link: download PDF or convert HTML to PDF, return metadata and file path.
#     Skip download if the file already exists."""
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link")
#             return None, None
        
#         # Use topic as fallback title if link.text is empty
#         title = link.text.strip() or topic or "Untitled"
        
#         # Clean title aggressively
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]
        
#         if not url.startswith('http'):
#             url = urljoin(page_url, url)
        
#         # Normalize dates
#         effective_date = normalize_date(effective_date)
#         modified_date = normalize_date(modified_date)
#         publish_date = normalize_date(publish_date)
        
#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg['url']}",
#             'posted_date': publish_date,  # Map to publish_date
#             'modified_date': modified_date,
#             'effective_date': effective_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }
        
#         # Extract clean base filename from URL (strip query params)
#         parsed_url = urlparse(url)
#         base_filename = os.path.basename(parsed_url.path)
#         if not base_filename:
#             base_filename = title + '.pdf'  # Fallback
#         else:
#             # Unquote and clean the base filename
#             base_filename = unquote(base_filename)
#             # Get extension from base filename, fallback to .pdf
#             _, ext = os.path.splitext(base_filename)
#             if not ext:
#                 ext = '.pdf'
        
#         # Sanitize the base filename for Windows
#         clean_base_filename = sanitize_filename(base_filename)
        
#         is_html = ext.lower() in ['.html', '.htm']
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, clean_base_filename)
        
#         # Check if file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, skipping download: {file_path}")
#             return metadata, file_path
        
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         # Log the actual file type based on extension
#         file_type = 'PDF' if ext.lower() == '.pdf' else ext.upper().lstrip('.')
#         logging.info(f"Downloading {file_type}: {url}")
        
#         if is_html:
#             try:
#                 import pdfkit
#                 pdfkit.from_url(url, file_path)
#                 logging.info(f"Converted HTML to PDF: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#                 return metadata, None
#         else:
#             try:
#                 response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
#                 response.raise_for_status()
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#                 logging.info(f"Downloaded {file_type}: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to download {file_type} from {url}: {e}")
#                 return metadata, None
        
#         return metadata, file_path
    
#     except Exception as e:
#         logging.error(f"Error processing document {url}: {e}")
#         return None, None

# def process_pdf(link, country_cfg, topic, page_url, effective_date=None):
#     """Process a document link: download PDF or convert HTML to PDF, return metadata and file path.
#     Skip download if the file already exists."""
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link")
#             return None, None
        
#         # Use topic as fallback title if link.text is empty
#         title = link.text.strip() or topic or "Untitled"
        
#         # Clean title aggressively
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_', '-', '(', ')', '[', ']')).replace(' ', '_')[:100]
        
#         if not url.startswith('http'):
#             url = urljoin(page_url, url)
        
#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg['url']}",
#             'posted_date': effective_date,
#             'modified_date': None,
#             'effective_date': effective_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }
        
#         # Extract clean base filename from URL (strip query params)
#         parsed_url = urlparse(url)
#         base_filename = os.path.basename(parsed_url.path)
#         if not base_filename:
#             base_filename = title + '.pdf'  # Fallback
#         else:
#             # Unquote and clean the base filename
#             base_filename = unquote(base_filename)
#             # Get extension from base filename, fallback to .pdf
#             _, ext = os.path.splitext(base_filename)
#             if not ext:
#                 ext = '.pdf'
        
#         # Sanitize the base filename for Windows
#         clean_base_filename = sanitize_filename(base_filename)
        
#         is_html = ext.lower() in ['.html', '.htm']
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, clean_base_filename)
        
#         # Check if file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, skipping download: {file_path}")
#             return metadata, file_path
        
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         # Log the actual file type based on extension
#         file_type = 'PDF' if ext.lower() == '.pdf' else ext.upper().lstrip('.')
#         logging.info(f"Downloading {file_type}: {url}")
        
#         if is_html:
#             try:
#                 import pdfkit
#                 pdfkit.from_url(url, file_path)
#                 logging.info(f"Converted HTML to PDF: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#                 return metadata, None
#         else:
#             try:
#                 response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)  # Increased timeout
#                 response.raise_for_status()
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#                 logging.info(f"Downloaded {file_type}: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to download {file_type} from {url}: {e}")
#                 return metadata, None
        
#         return metadata, file_path
    
#     except Exception as e:
#         logging.error(f"Error processing document {url}: {e}")
#         return None, None  # Return None for metadata too on error

# def normalize_date(date_str):
#     """Normalize a date string to YYYY-MM-DD format."""
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.strftime('%Y-%m-%d')
#     except Exception as e:
#         logging.error(f"Failed to parse date {date_str}: {e}")
#         return None

# import os
# from dotenv import load_dotenv
# import requests
# import logging
# from urllib.parse import urljoin
# from datetime import datetime
# import dateutil.parser
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from __init__ import ldb  # Import LeximGPTDb instance

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def get_db_connection_str():
#     """Return the database connection string using LeximGPTDb."""
#     conn_str = ldb.get_connection_str()
#     logging.info(f"Database connection string: {conn_str}")
#     return conn_str

# def process_pdf(link, country_cfg, topic, page_url, effective_date=None):
#     """Process a document link: download PDF or convert HTML to PDF, return metadata and file path.
#     Skip download if the file already exists."""
#     try:
#         url = link.get('href')
#         if not url:
#             logging.error("No href found in link")
#             return None, None
        
#         title = link.text.strip() or "Untitled"
#         title = ''.join(c for c in title if c.isalnum() or c in (' ', '_')).replace(' ', '_')[:100]
        
#         if not url.startswith('http'):
#             url = urljoin(page_url, url)
        
#         metadata = {
#             'title': title,
#             'url': url,
#             'description': f"{topic} document from {country_cfg['url']}",
#             'posted_date': effective_date,
#             'modified_date': None,
#             'effective_date': effective_date,
#             'country': country_cfg.get('country', 'Unknown')
#         }
        
#         ext = os.path.splitext(url)[1].lower()
#         is_html = ext in ['.html', '.htm']
#         temp_dir = os.environ.get('TEMP', 'C:\\Temp')
#         os.makedirs(temp_dir, exist_ok=True)
#         file_path = os.path.join(temp_dir, f"{title}{'.pdf' if is_html else ext}")
        
#         # Check if file already exists
#         if os.path.exists(file_path):
#             logging.info(f"File already exists, skipping download: {file_path}")
#             return metadata, file_path
        
#         session = requests.Session()
#         retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         if is_html:
#             try:
#                 import pdfkit
#                 pdfkit.from_url(url, file_path)
#                 logging.info(f"Converted HTML to PDF: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to convert HTML to PDF for {url}: {e}")
#                 return metadata, None
#         else:
#             try:
#                 response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
#                 response.raise_for_status()
#                 with open(file_path, 'wb') as f:
#                     f.write(response.content)
#                 logging.info(f"Downloaded PDF: {file_path}")
#             except Exception as e:
#                 logging.error(f"Failed to download PDF from {url}: {e}")
#                 return metadata, None
        
#         return metadata, file_path
    
#     except Exception as e:
#         logging.error(f"Error processing document {url}: {e}")
#         return metadata, None

# def normalize_date(date_str):
#     """Normalize a date string to YYYY-MM-DD format."""
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.strftime('%Y-%m-%d')
#     except Exception as e:
#         logging.error(f"Failed to parse date {date_str}: {e}")
#         return None

