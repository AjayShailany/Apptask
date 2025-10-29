import os
import logging
import argparse
from datetime import datetime
import dateutil.parser
from config import Config
from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
from Nigeria import scrape_nigeria
from south_africa import scrape_south_africa
from Singapore import scrape_singapore
from thailand import scrape_thailand
from ireland import scrape_ireland
from canada import scrape_canada
from belgium import scrape_belgium

# Mapping of country names to their respective scraping functions
SCRAPE_FUNCTIONS = {
    'Nigeria': scrape_nigeria,
    'south_africa': scrape_south_africa,
    'Singapore': scrape_singapore,
    'Thailand': scrape_thailand,
    'Ireland': scrape_ireland,
    'Canada': scrape_canada,
    'Belgium': scrape_belgium
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_date(date_str):
    if not date_str:
        return None
    try:
        parsed = dateutil.parser.parse(date_str)
        return parsed.date()
    except Exception as e:
        logging.error(f"Date parse failed: {date_str} - {e}")
        return None

def run_pipeline(country=None):
    # Determine countries to process
    if country:
        # Case-insensitive lookup for SCRAPE_FUNCTIONS
        country_key = None
        for key in SCRAPE_FUNCTIONS:
            if key.lower() == country.lower():
                country_key = key
                break
        if country_key:
            countries = [country_key]
        else:
            logging.error(f"Invalid country: {country}. Processing all countries: {list(SCRAPE_FUNCTIONS.keys())}")
            countries = list(SCRAPE_FUNCTIONS.keys())
    else:
        countries = list(SCRAPE_FUNCTIONS.keys())
        logging.info(f"No country specified. Processing all countries: {countries}")
    
    for country in countries:
        scrape_function = SCRAPE_FUNCTIONS.get(country)
        if not scrape_function:
            logging.error(f"No scrape function defined for {country}")
            continue
        
        country_cfg = Config.get_country_config(country)
        if not country_cfg:
            logging.error(f"No config for {country}")
            continue
        
        starting_docket_id = country_cfg['starting_docket_id']
        last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
        next_docket_id = last_docket_id + 1
        
        latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
        try:
            data_list = scrape_function(country_cfg)
            if data_list is None:
                logging.error(f"Scrape function for {country} returned None")
                data_list = []
        except Exception as e:
            logging.error(f"Scraping failed for {country}: {e}")
            data_list = []
        
        # Process and insert data
        logging.info(f"Scraped data for {country}:")
        if not data_list:
            logging.info("No data scraped.")
        else:
            for i, (metadata, file_path) in enumerate(data_list, 1):
                logging.info(f"Item {i}:")
                logging.info(f"  Metadata: {metadata}")
                logging.info(f"  File Path: {file_path}")
                
                try:
                    title = metadata['title']
                    url = metadata['url']
                    
                    if check_duplicate(title, url):
                        logging.info(f"Duplicate found: {title} - {url}")
                        if file_path and os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                logging.info(f"Deleted duplicate temporary file: {file_path}")
                            except Exception as e:
                                logging.error(f"Failed to delete {file_path}: {e}")
                        continue
                    
                    # Use normalized dates from metadata
                    posted_date = metadata.get('posted_date')
                    modified_date = metadata.get('modified_date')
                    effective_date = metadata.get('effective_date')
                    
                    # Convert to datetime.date for comparison
                    date_to_check = None
                    for date in [modified_date, posted_date, effective_date]:
                        if date:
                            try:
                                date_to_check = datetime.strptime(date, '%Y-%m-%d').date() if isinstance(date, str) else date
                                break
                            except (ValueError, TypeError) as e:
                                logging.error(f"Invalid date format for comparison: {date} - {e}")
                                continue
                    
                    if latest_date and date_to_check and date_to_check <= latest_date:
                        logging.info(f"Old data, skipping: {title}")
                        if file_path and os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                logging.info(f"Deleted old temporary file: {file_path}")
                            except Exception as e:
                                logging.error(f"Failed to delete {file_path}: {e}")
                        continue
                    
                    docket_id_str = str(next_docket_id)
                    doc_id = f"{docket_id_str}-01"
                    
                    insert_data = {
                        'country': country,
                        'docket_id': docket_id_str,
                        'doc_id': doc_id,
                        'document_type': 'Guidelines',
                        'agency_id': country_cfg.get('agency_id', ''),
                        'reference': None,
                        'title': title,
                        'url': url,
                        'abstract': metadata.get('description', ''),
                        'program_id': str(Config.PROGRAM_ID),
                        'publish_date': posted_date,
                        'modified_date': modified_date,
                        'effective_date': effective_date,
                        'doc_format': Config.DOCUMENT_FORMAT,
                        'in_elastic': False,
                        'create_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'modified_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    if insert_metadata(insert_data):
                        logging.info(f"Inserted: {title}")
                        next_docket_id += 1
                        if file_path and os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                logging.info(f"Deleted temporary file: {file_path}")
                            except Exception as e:
                                logging.error(f"Failed to delete {file_path}: {e}")
                    else:
                        logging.error(f"Failed to insert: {title}")
                
                except Exception as e:
                    logging.error(f"Error processing item for {country}: {e}")
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            logging.info(f"Deleted temporary file on error: {file_path}")
                        except Exception as e:
                            logging.error(f"Failed to delete {file_path}: {e}")
                    continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline for a specific country or all countries.")
    parser.add_argument('--country', type=str, help="Country to process (e.g., Nigeria, south_africa, Singapore, Thailand, Ireland, Canada). Omit to process all countries.")
    args = parser.parse_args()
    
    run_pipeline(args.country)
# import os
# import logging
# import argparse
# from datetime import datetime
# import dateutil.parser
# from config import Config
# from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
# from Nigeria import scrape_nigeria
# from south_africa import scrape_south_africa
# from Singapore import scrape_singapore
# from thailand import scrape_thailand
# from ireland import scrape_ireland
# from canada import scrape_canada

# # Mapping of country names to their respective scraping functions
# SCRAPE_FUNCTIONS = {
#     'Nigeria': scrape_nigeria,
#     'south_africa': scrape_south_africa,
#     'Singapore': scrape_singapore,
#     'thailand': scrape_thailand,
#     'ireland': scrape_ireland,
#     'canada': scrape_canada,
    
    
    
# }

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def normalize_date(date_str):
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.date()
#     except Exception as e:
#         logging.error(f"Date parse failed: {date_str} - {e}")
#         return None

# def run_pipeline(country=None):
#     # Determine countries to process
#     if country and country in SCRAPE_FUNCTIONS:
#         countries = [country]
#     else:
#         countries = list(SCRAPE_FUNCTIONS.keys())
#         if country:
#             logging.error(f"Invalid country: {country}. Processing all countries: {countries}")
#         else:
#             logging.info(f"No country specified. Processing all countries: {countries}")
    
#     for country in countries:
#         scrape_function = SCRAPE_FUNCTIONS.get(country)
#         if not scrape_function:
#             logging.error(f"No scrape function defined for {country}")
#             continue
        
#         country_cfg = Config.get_country_config(country)
#         if not country_cfg:
#             logging.error(f"No config for {country}")
#             continue
        
#         starting_docket_id = country_cfg['starting_docket_id']
#         last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
#         next_docket_id = last_docket_id + 1
        
#         latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
#         try:
#             data_list = scrape_function(country_cfg)
#             if data_list is None:
#                 logging.error(f"Scrape function for {country} returned None")
#                 data_list = []
#         except Exception as e:
#             logging.error(f"Scraping failed for {country}: {e}")
#             data_list = []
        
#         # Display scraped data
#         logging.info(f"Scraped data for {country}:")
#         if not data_list:
#             logging.info("No data scraped.")
#         else:
#             for i, (metadata, file_path) in enumerate(data_list, 1):
#                 logging.info(f"Item {i}:")
#                 logging.info(f"  Metadata: {metadata}")
#                 logging.info(f"  File Path: {file_path}")
                
#                 # Optional: Process and insert data (commented out for display-only mode)
#                 """
#                 try:
#                     title = metadata['title']
#                     url = metadata['url']
                    
#                     if check_duplicate(title, url):
#                         logging.info(f"Duplicate found: {title} - {url}")
#                         continue
                    
#                     posted_date = normalize_date(metadata.get('posted_date'))
#                     modified_date = normalize_date(metadata.get('modified_date'))
#                     effective_date = normalize_date(metadata.get('effective_date'))
                    
#                     date_to_check = modified_date or posted_date or effective_date
#                     if latest_date and date_to_check and date_to_check <= latest_date:
#                         logging.info(f"Old data, skipping: {title}")
#                         continue
                    
#                     docket_id_str = str(next_docket_id)
#                     doc_id = f"{docket_id_str}-01"
                    
#                     insert_data = {
#                         'country': country,
#                         'docket_id': docket_id_str,
#                         'doc_id': doc_id,
#                         'document_type': 'Guidelines',
#                         'agency_id': country_cfg.get('agency_id', ''),
#                         'reference': None,
#                         'title': title,
#                         'url': url,
#                         'abstract': metadata.get('description', ''),
#                         'program_id': str(Config.PROGRAM_ID),
#                         'publish_date': posted_date.strftime('%Y-%m-%d') if posted_date else None,
#                         'modified_date': modified_date.strftime('%Y-%m-%d') if modified_date else None,
#                         'effective_date': effective_date.strftime('%Y-%m-%d') if effective_date else None,
#                         'doc_format': Config.DOCUMENT_FORMAT,
#                         'in_elastic': False,
#                         'create_date': datetime.now(),
#                         'modified_date': datetime.now()
#                     }
                    
#                     if insert_metadata(insert_data):
#                         logging.info(f"Inserted: {title}")
#                         next_docket_id += 1
#                         if file_path:
#                             try:
#                                 os.remove(file_path)
#                                 logging.info(f"Deleted temporary file: {file_path}")
#                             except Exception as e:
#                                 logging.error(f"Failed to delete {file_path}: {e}")
#                 except Exception as e:
#                     logging.error(f"Error processing item for {country}: {e}")
#                     continue
#                 """

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Run the pipeline for a specific country or all countries.")
#     parser.add_argument('--country', type=str, help="Country to process (e.g., Nigeria, south_africa, Singapore). Omit to process all countries.")
#     args = parser.parse_args()
    
#     run_pipeline(args.country)
# import os
# import logging
# import argparse
# from datetime import datetime
# import dateutil.parser
# from config import Config
# from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
# from Nigeria import scrape_nigeria
# from south_africa import scrape_south_africa

# # Mapping of country names to their respective scraping functions
# SCRAPE_FUNCTIONS = {
#     'Nigeria': scrape_nigeria,
#     'south_africa': scrape_south_africa,
# }

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def normalize_date(date_str):
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.date()
#     except Exception as e:
#         logging.error(f"Date parse failed: {date_str} - {e}")
#         return None

# def run_pipeline(country=None):
#     # Determine countries to process
#     if country and country in SCRAPE_FUNCTIONS:
#         countries = [country]
#     else:
#         countries = list(SCRAPE_FUNCTIONS.keys())
#         if country:
#             logging.error(f"Invalid country: {country}. Processing all countries: {countries}")
#         else:
#             logging.info(f"No country specified. Processing all countries: {countries}")
    
#     for country in countries:
#         scrape_function = SCRAPE_FUNCTIONS.get(country)
#         if not scrape_function:
#             logging.error(f"No scrape function defined for {country}")
#             continue
        
#         country_cfg = Config.get_country_config(country)
#         if not country_cfg:
#             logging.error(f"No config for {country}")
#             continue
        
#         starting_docket_id = country_cfg['starting_docket_id']
#         last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
#         next_docket_id = last_docket_id + 1
        
#         latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
#         try:
#             data_list = scrape_function(country_cfg)
#             if data_list is None:
#                 logging.error(f"Scrape function for {country} returned None")
#                 data_list = []
#         except Exception as e:
#             logging.error(f"Scraping failed for {country}: {e}")
#             data_list = []
        
#         # Display scraped data
#         logging.info(f"Scraped data for {country}:")
#         if not data_list:
#             logging.info("No data scraped.")
#         else:
#             for i, (metadata, file_path) in enumerate(data_list, 1):
#                 logging.info(f"Item {i}:")
#                 logging.info(f"  Metadata: {metadata}")
#                 logging.info(f"  File Path: {file_path}")
                
#                 # Optional: Process and insert data (commented out for display-only mode)
#                 """
#                 try:
#                     title = metadata['title']
#                     url = metadata['url']
                    
#                     if check_duplicate(title, url):
#                         logging.info(f"Duplicate found: {title} - {url}")
#                         continue
                    
#                     posted_date = normalize_date(metadata.get('posted_date'))
#                     modified_date = normalize_date(metadata.get('modified_date'))
#                     effective_date = normalize_date(metadata.get('effective_date'))
                    
#                     date_to_check = modified_date or posted_date or effective_date
#                     if latest_date and date_to_check and date_to_check <= latest_date:
#                         logging.info(f"Old data, skipping: {title}")
#                         continue
                    
#                     docket_id_str = str(next_docket_id)
#                     doc_id = f"{docket_id_str}-01"
                    
#                     insert_data = {
#                         'country': country,
#                         'docket_id': docket_id_str,
#                         'doc_id': doc_id,
#                         'document_type': 'Guidelines',
#                         'agency_id': country_cfg.get('agency_id', ''),
#                         'reference': None,
#                         'title': title,
#                         'url': url,
#                         'abstract': metadata.get('description', ''),
#                         'program_id': str(Config.PROGRAM_ID),
#                         'publish_date': posted_date.strftime('%Y-%m-%d') if posted_date else None,
#                         'modified_date': modified_date.strftime('%Y-%m-%d') if modified_date else None,
#                         'effective_date': effective_date.strftime('%Y-%m-%d') if effective_date else None,
#                         'doc_format': Config.DOCUMENT_FORMAT,
#                         'in_elastic': False,
#                         'create_date': datetime.now(),
#                         'modified_date': datetime.now()
#                     }
                    
#                     if insert_metadata(insert_data):
#                         logging.info(f"Inserted: {title}")
#                         next_docket_id += 1
#                         if file_path:
#                             try:
#                                 os.remove(file_path)
#                                 logging.info(f"Deleted temporary file: {file_path}")
#                             except Exception as e:
#                                 logging.error(f"Failed to delete {file_path}: {e}")
#                 except Exception as e:
#                     logging.error(f"Error processing item for {country}: {e}")
#                     continue
#                 """

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Run the pipeline for a specific country or all countries.")
#     parser.add_argument('--country', type=str, help="Country to process (e.g., Nigeria, south_africa). Omit to process all countries.")
#     args = parser.parse_args()
    
#     run_pipeline(args.country)

# import os
# import logging
# from datetime import datetime
# import dateutil.parser
# from hashlib import md5
# from config import Config
# from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
# from Nigeria import scrape_nigeria
# from south_africa import scrape_south_africa  # Ensure this module exists

# # Mapping of country names to their respective scraping functions
# SCRAPE_FUNCTIONS = {
#     'Nigeria': scrape_nigeria,
#     'south_africa': scrape_south_africa,
# }

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def normalize_date(date_str):
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.date()
#     except Exception as e:
#         logging.error(f"Date parse failed: {date_str} - {e}")
#         return None

# def run_pipeline():
#     countries = ['Nigeria', 'south_africa']
    
#     for country in countries:
#         scrape_function = SCRAPE_FUNCTIONS.get(country)
#         if not scrape_function:
#             logging.error(f"No scrape function defined for {country}")
#             continue
        
#         country_cfg = Config.get_country_config(country)
#         if not country_cfg:
#             logging.error(f"No config for {country}")
#             continue
        
#         starting_docket_id = country_cfg['starting_docket_id']
#         last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
#         next_docket_id = last_docket_id + 1
        
#         latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
#         try:
#             data_list = scrape_function(country_cfg)
#             if data_list is None:
#                 logging.error(f"Scrape function for {country} returned None")
#                 data_list = []  # Default to empty list to avoid iteration error
#         except Exception as e:
#             logging.error(f"Scraping failed for {country}: {e}")
#             data_list = []  # Default to empty list on error
        
#         for item in data_list:
#             try:
#                 metadata, file_path = item
#                 title = metadata['title']
#                 url = metadata['url']
                
#                 if check_duplicate(title, url):
#                     logging.info(f"Duplicate found: {title} - {url}")
#                     continue
                
#                 posted_date = normalize_date(metadata.get('posted_date'))
#                 modified_date = normalize_date(metadata.get('modified_date'))
#                 effective_date = normalize_date(metadata.get('effective_date'))
                
#                 date_to_check = modified_date or posted_date or effective_date
#                 if latest_date and date_to_check and date_to_check <= latest_date:
#                     logging.info(f"Old data, skipping: {title}")
#                     continue
                
#                 docket_id_str = str(next_docket_id)
#                 doc_id = f"{docket_id_str}-01"
                
#                 insert_data = {
#                     'country': country,
#                     'docket_id': docket_id_str,
#                     'doc_id': doc_id,
#                     'document_type': 'Guidelines',
#                     'agency_id': country_cfg.get('agency_id', ''),
#                     'reference': None,
#                     'title': title,
#                     'url': url,
#                     'abstract': metadata.get('description', ''),
#                     'program_id': str(Config.PROGRAM_ID),
#                     'publish_date': posted_date.strftime('%Y-%m-%d') if posted_date else None,
#                     'modified_date': modified_date.strftime('%Y-%m-%d') if modified_date else None,
#                     'effective_date': effective_date.strftime('%Y-%m-%d') if effective_date else None,
#                     'doc_format': Config.DOCUMENT_FORMAT,
#                     'in_elastic': False,
#                     'create_date': datetime.now(),
#                     'modified_date': datetime.now()
#                 }
                
#                 if insert_metadata(insert_data):
#                     logging.info(f"Inserted: {title}")
#                     next_docket_id += 1
#                     if file_path:
#                         try:
#                             os.remove(file_path)
#                             logging.info(f"Deleted temporary file: {file_path}")
#                         except Exception as e:
#                             logging.error(f"Failed to delete {file_path}: {e}")
#             except Exception as e:
#                 logging.error(f"Error processing item for {country}: {e}")
#                 continue

# if __name__ == "__main__":
#     run_pipeline()


# import os
# import logging
# from datetime import datetime
# import dateutil.parser
# from hashlib import md5
# from config import Config
# from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
# from Nigeria import scrape_nigeria
# from south_africa import scrape_south_africa  # Assuming you have this module

# # Mapping of country names to their respective scraping functions
# SCRAPE_FUNCTIONS = {
#     'Nigeria': scrape_nigeria,
#     'south_africa': scrape_south_africa,
#     # Add more countries and their scrape functions here
# }

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def normalize_date(date_str):
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.date()
#     except Exception as e:
#         logging.error(f"Date parse failed: {date_str} - {e}")
#         return None

# def run_pipeline():
#     countries = ['Nigeria', 'south_africa']  # Add more countries as needed
    
#     for country in countries:
#         # Check if a scrape function exists for the country
#         scrape_function = SCRAPE_FUNCTIONS.get(country)
#         if not scrape_function:
#             logging.error(f"No scrape function defined for {country}")
#             continue
        
#         # Get country-specific configuration
#         country_cfg = Config.get_country_config(country)
#         if not country_cfg:
#             logging.error(f"No config for {country}")
#             continue
        
#         # Get the last docket ID or use the starting ID from config
#         starting_docket_id = country_cfg['starting_docket_id']
#         last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
#         next_docket_id = last_docket_id + 1
        
#         # Get the latest date for filtering
#         latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
#         # Call the country-specific scrape function
#         try:
#             data_list = scrape_function(country_cfg)
#         except Exception as e:
#             logging.error(f"Scraping failed for {country}: {e}")
#             continue
        
#         # Process scraped data
#         for item in data_list:
#             metadata, file_path = item
#             title = metadata['title']
#             url = metadata['url']
            
#             # Check for duplicates
#             if check_duplicate(title, url):
#                 logging.info(f"Duplicate found: {title} - {url}")
#                 continue
            
#             # Normalize dates
#             posted_date = normalize_date(metadata.get('posted_date'))
#             modified_date = normalize_date(metadata.get('modified_date'))
#             effective_date = normalize_date(metadata.get('effective_date'))
            
#             # Skip old data
#             date_to_check = modified_date or posted_date or effective_date
#             if latest_date and date_to_check and date_to_check <= latest_date:
#                 logging.info(f"Old data, skipping: {title}")
#                 continue
            
#             # Prepare data for insertion
#             docket_id_str = str(next_docket_id)
#             doc_id = f"{docket_id_str}-01"
            
#             insert_data = {
#                 'country': country,
#                 'docket_id': docket_id_str,
#                 'doc_id': doc_id,
#                 'document_type': 'Guidelines',
#                 'agency_id': country_cfg.get('agency_id', ''),
#                 'reference': None,
#                 'title': title,
#                 'url': url,
#                 'abstract': metadata.get('description', ''),
#                 'program_id': str(Config.PROGRAM_ID),
#                 'publish_date': posted_date.strftime('%Y-%m-%d') if posted_date else None,
#                 'modified_date': modified_date.strftime('%Y-%m-%d') if modified_date else None,
#                 'effective_date': effective_date.strftime('%Y-%m-%d') if effective_date else None,
#                 'doc_format': Config.DOCUMENT_FORMAT,
#                 'in_elastic': False,
#                 'create_date': datetime.now(),
#                 'modified_date': datetime.now()
#             }
            
#             # Insert metadata into the database
#             if insert_metadata(insert_data):
#                 logging.info(f"Inserted: {title}")
#                 next_docket_id += 1
#                 if file_path:
#                     try:
#                         os.remove(file_path)  # Clean up local file
#                     except Exception as e:
#                         logging.error(f"Failed to delete {file_path}: {e}")

# if __name__ == "__main__":
#     run_pipeline()

# import os
# import logging
# from datetime import datetime
# import dateutil.parser
# from hashlib import md5
# from config import Config
# from DatabaseOps import get_last_docket_id, get_latest_date, check_duplicate, insert_metadata
# from Nigeria import scrape_nigeria

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def normalize_date(date_str):
#     if not date_str:
#         return None
#     try:
#         parsed = dateutil.parser.parse(date_str)
#         return parsed.date()
#     except Exception as e:
#         logging.error(f"Date parse failed: {date_str} - {e}")
#         return None

# def run_pipeline():
#     countries = ['Nigeria','south_africa']
    
#     for country in countries:
#         country_cfg = Config.get_country_config(country)
#         if not country_cfg:
#             logging.error(f"No config for {country}")
#             continue
        
#         starting_docket_id = country_cfg['starting_docket_id']
#         last_docket_id = get_last_docket_id(Config.PROGRAM_ID, country) or (starting_docket_id - 1)
#         next_docket_id = last_docket_id + 1
        
#         latest_date = get_latest_date(Config.PROGRAM_ID, country)
        
#         data_list = scrape_nigeria(country_cfg)
        
#         for item in data_list:
#             metadata, file_path = item
#             title = metadata['title']
#             url = metadata['url']
#             if check_duplicate(title, url):
#                 logging.info(f"Duplicate found: {title} - {url}")
#                 continue
            
#             posted_date = normalize_date(metadata.get('posted_date'))
#             modified_date = normalize_date(metadata.get('modified_date'))
#             effective_date = normalize_date(metadata.get('effective_date'))
            
#             date_to_check = modified_date or posted_date or effective_date
#             if latest_date and date_to_check and date_to_check <= latest_date:
#                 logging.info(f"Old data, skipping: {title}")
#                 continue
            
#             docket_id_str = str(next_docket_id)
#             doc_id = f"{docket_id_str}-01"
            
#             insert_data = {
#                 'country': country,
#                 'docket_id': docket_id_str,
#                 'doc_id': doc_id,
#                 'document_type': 'Guidelines',
#                 'agency_id': country_cfg.get('agency_id', ''),
#                 'reference': None,
#                 'title': title,
#                 'url': url,
#                 'abstract': metadata.get('description', ''),
#                 'program_id': str(Config.PROGRAM_ID),
#                 'publish_date': posted_date.strftime('%Y-%m-%d') if posted_date else None,
#                 'modified_date': modified_date.strftime('%Y-%m-%d') if modified_date else None,
#                 'effective_date': effective_date.strftime('%Y-%m-%d') if effective_date else None,
#                 'doc_format': Config.DOCUMENT_FORMAT,
#                 'in_elastic': False,
#                 'create_date': datetime.now(),
#                 'modified_date': datetime.now()
#             }
            
#             if insert_metadata(insert_data):
#                 logging.info(f"Inserted: {title}")
#                 next_docket_id += 1
#                 if file_path:
#                     try:
#                         os.remove(file_path)  # Clean up local file
#                     except Exception as e:
#                         logging.error(f"Failed to delete {file_path}: {e}")

# if __name__ == "__main__":
#     run_pipeline()
