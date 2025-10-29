from dotenv import load_dotenv
import pandas as pd
import logging
from sqlalchemy import text, create_engine
from hashlib import md5
from datetime import datetime
import dateutil.parser
from utils import get_db_connection_str

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_NAME = 'international_documents'

def get_engine():
    """Create and return a new SQLAlchemy engine."""
    conn_str = get_db_connection_str()
    return create_engine(conn_str)

def run_query(query_str, params=None):
    """Execute a SQL query with optional parameters using SQLAlchemy."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query_str), params or {})
            return True, result.fetchall()
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return False, None
    finally:
        engine.dispose()

def run_query_insert_update(query_str, params=None):
    """Execute a SQL query with optional parameters."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query_str), params) if params else conn.execute(text(query_str))
            query_type = query_str.strip().upper().split()[0]
            if query_type in ("INSERT", "UPDATE", "DELETE"):
                conn.commit()
                return True, None
            elif query_type == "SELECT":
                return True, result.fetchall()
            return True, None
    except Exception as e:
        logging.error(f"Database error: {e}")
        return False, str(e)
    finally:
        engine.dispose()

def run_query_to_df(query_str, params=None):
    """Execute a query and return the result as a Pandas DataFrame."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query_str), conn, params=params or {})
            return True, result
    except Exception as e:
        logging.error(f"Query to DataFrame failed: {e}")
        return False, None
    finally:
        engine.dispose()

def run_query_to_list_of_dicts(query_str, params=None):
    """Execute a SQL query and return results as a list of dictionaries."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query_str), conn, params=params or {})
            result_list = result.to_dict(orient="records")
            return True, result_list
    except Exception as e:
        logging.error(f"Query to list of dicts failed: {e}")
        return False, None
    finally:
        engine.dispose()

def get_last_docket_id(program_id, country):
    """Get the highest docket_id for a given program and country."""
    query = """
    SELECT MAX(CAST(docket_id AS UNSIGNED)) AS max_docket 
    FROM international_documents 
    WHERE program_id = :program_id AND country = :country
    """
    success, result = run_query_to_list_of_dicts(query, {'program_id': str(program_id), 'country': country})
    if success and result and result[0].get('max_docket') is not None:
        return int(result[0]['max_docket'])
    return None

def get_latest_date(program_id, country):
    """Get the latest date (modified, publish, or effective) for a given program and country."""
    query = """
    SELECT MAX(GREATEST(
        IFNULL(STR_TO_DATE(modified_date, '%Y-%m-%d'), '1900-01-01'),
        IFNULL(STR_TO_DATE(effective_date, '%Y-%m-%d'), '1900-01-01'),
        IFNULL(STR_TO_DATE(publish_date, '%Y-%m-%d'), '1900-01-01')
    )) AS latest_date 
    FROM international_documents 
    WHERE program_id = :program_id AND country = :country
    """
    success, result = run_query_to_df(query, {'program_id': str(program_id), 'country': country})
    if success and not result.empty:
        latest = result.iloc[0]['latest_date']
        return latest if latest and latest != datetime(1900, 1, 1) else None
    return None

def check_duplicate(title, url):
    """Check if a document is a duplicate based on title and URL hash."""
    if not title or not url:
        logging.warning("Title or URL is None or empty; cannot check duplicate.")
        return True
    doc_hash = md5((title + url).encode()).hexdigest()
    query = """
    SELECT COUNT(*) AS count 
    FROM international_documents 
    WHERE doc_hash = :doc_hash
    """
    success, result = run_query_to_list_of_dicts(query, {'doc_hash': doc_hash})
    if success and result:
        return result[0]['count'] > 0
    return False

def insert_metadata(metadata):
    """Insert metadata into the database, using publish_date for posted_date."""
    required_fields = [
        'country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format',
        'in_elastic', 'create_date', 'modified_date'
    ]
    for field in required_fields:
        if field not in metadata or metadata[field] is None:
            metadata[field] = metadata.get(field, '') if field in ['country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format'] else None
            if field in ['create_date', 'modified_date']:
                metadata[field] = datetime.now()

    # Add optional fields with defaults
    metadata['doc_hash'] = md5((metadata['title'] + metadata['url']).encode()).hexdigest()
    metadata['document_type'] = metadata.get('document_type', '')
    metadata['agency_id'] = metadata.get('agency_id', '')
    metadata['reference'] = metadata.get('reference', '')
    metadata['abstract'] = metadata.get('description', '')  # Map description to abstract
    metadata['modified_date'] = metadata.get('modified_date')
    metadata['effective_date'] = metadata.get('effective_date')
    # Use posted_date for publish_date as the single date column
    metadata['publish_date'] = metadata.get('posted_date')  # Map posted_date to publish_date
    metadata['in_elastic'] = metadata.get('in_elastic', 0)  # Default to 0 if not set

    # Remove other date fields to avoid confusion
    for key in ['public_date', 'effective_date']:
        if key in metadata:
            del metadata[key]

    query = """
    INSERT INTO international_documents (
        country, docket_id, doc_id, doc_hash, document_type, agency_id, reference, title, url, abstract,
        program_id, modified_date, publish_date, doc_format, in_elastic, create_date
    ) VALUES (
        :country, :docket_id, :doc_id, :doc_hash, :document_type, :agency_id, :reference, :title, :url, :abstract,
        :program_id, :modified_date, :publish_date, :doc_format, :in_elastic, :create_date
    )
    """
    success, error = run_query_insert_update(query, metadata)
    if not success:
        logging.error(f"Failed to insert metadata: {error}")
    else:
        logging.info(f"Inserted: {metadata['title']}")
    return success


# from dotenv import load_dotenv
# import pandas as pd
# import logging
# from sqlalchemy import text, create_engine
# from hashlib import md5
# from datetime import datetime
# import dateutil.parser
# from utils import get_db_connection_str

# load_dotenv(override=True)

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# TABLE_NAME = 'international_documents'

# def get_engine():
#     """Create and return a new SQLAlchemy engine."""
#     conn_str = get_db_connection_str()
#     return create_engine(conn_str)

# def run_query(query_str, params=None):
#     """Execute a SQL query with optional parameters using SQLAlchemy."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query_str), params or {})
#             return True, result.fetchall()
#     except Exception as e:
#         logging.error(f"Query failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# def run_query_insert_update(query_str, params=None):
#     """Execute a SQL query with optional parameters."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query_str), params) if params else conn.execute(text(query_str))
#             query_type = query_str.strip().upper().split()[0]
#             if query_type in ("INSERT", "UPDATE", "DELETE"):
#                 conn.commit()
#                 return True, None
#             elif query_type == "SELECT":
#                 return True, result.fetchall()
#             return True, None
#     except Exception as e:
#         logging.error(f"Database error: {e}")
#         return False, str(e)
#     finally:
#         engine.dispose()

# def run_query_to_df(query_str, params=None):
#     """Execute a query and return the result as a Pandas DataFrame."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = pd.read_sql_query(text(query_str), conn, params=params or {})
#             return True, result
#     except Exception as e:
#         logging.error(f"Query to DataFrame failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# def run_query_to_list_of_dicts(query_str, params=None):
#     """Execute a SQL query and return results as a list of dictionaries."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = pd.read_sql_query(text(query_str), conn, params=params or {})
#             result_list = result.to_dict(orient="records")
#             return True, result_list
#     except Exception as e:
#         logging.error(f"Query to list of dicts failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# def get_last_docket_id(program_id, country):
#     """Get the highest docket_id for a given program and country."""
#     query = """
#     SELECT MAX(CAST(docket_id AS UNSIGNED)) AS max_docket 
#     FROM international_documents 
#     WHERE program_id = :program_id AND country = :country
#     """
#     success, result = run_query_to_list_of_dicts(query, {'program_id': str(program_id), 'country': country})
#     if success and result and result[0].get('max_docket') is not None:
#         return int(result[0]['max_docket'])
#     return None

# def get_latest_date(program_id, country):
#     """Get the latest date (modified, publish, or effective) for a given program and country."""
#     query = """
#     SELECT MAX(GREATEST(
#         IFNULL(STR_TO_DATE(modified_date, '%Y-%m-%d'), '1900-01-01'),
#         IFNULL(STR_TO_DATE(publish_date, '%Y-%m-%d'), '1900-01-01'),
#         IFNULL(STR_TO_DATE(effective_date, '%Y-%m-%d'), '1900-01-01')
#     )) AS latest_date 
#     FROM international_documents 
#     WHERE program_id = :program_id AND country = :country
#     """
#     success, result = run_query_to_df(query, {'program_id': str(program_id), 'country': country})
#     if success and not result.empty:
#         latest = result.iloc[0]['latest_date']
#         return latest if latest and latest != datetime(1900, 1, 1) else None
#     return None

# def check_duplicate(title, url):
#     """Check if a document is a duplicate based on title and URL hash."""
#     if not title or not url:
#         logging.warning("Title or URL is None or empty; cannot check duplicate.")
#         return True
#     doc_hash = md5((title + url).encode()).hexdigest()
#     query = """
#     SELECT COUNT(*) AS count 
#     FROM international_documents 
#     WHERE doc_hash = :doc_hash
#     """
#     success, result = run_query_to_list_of_dicts(query, {'doc_hash': doc_hash})
#     if success and result:
#         return result[0]['count'] > 0
#     return False

# def insert_metadata(metadata):
#     """Insert metadata into the database."""
#     required_fields = [
#         'country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format',
#         'in_elastic', 'create_date', 'modified_date'
#     ]
#     for field in required_fields:
#         if field not in metadata or metadata[field] is None:
#             metadata[field] = metadata.get(field, '') if field in ['country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format'] else None
#             if field in ['create_date', 'modified_date']:
#                 metadata[field] = datetime.now()
    
#     metadata['doc_hash'] = md5((metadata['title'] + metadata['url']).encode()).hexdigest()
    
#     query = """
#     INSERT INTO international_documents (
#         country, docket_id, doc_id, doc_hash, document_type, agency_id, reference, title, url, abstract, 
#         program_id, publish_date, modified_date, effective_date, doc_format, in_elastic, create_date
#     ) VALUES (
#         :country, :docket_id, :doc_id, :doc_hash, :document_type, :agency_id, :reference, :title, :url, :abstract, 
#         :program_id, :publish_date, :modified_date, :effective_date, :doc_format, :in_elastic, :create_date
#     )
#     """
#     success, error = run_query_insert_update(query, metadata)
#     if not success:
#         logging.error(f"Failed to insert metadata: {error}")
#     return success
# from dotenv import load_dotenv
# import pandas as pd
# import logging
# from sqlalchemy import text, create_engine
# from hashlib import md5
# from datetime import datetime
# import dateutil.parser  # For parsing dates

# from utils import get_rds_connection_str  # As per provided

# load_dotenv(override=True)

# TABLE_NAME = 'international_document'  # Assumed table name; change if different

# def get_engine():
#     """Create and return a new SQLAlchemy engine."""
#     conn_str = get_rds_connection_str()
#     return create_engine(conn_str)

# # Provided database utility functions (assumed included from user's original code)
# def run_query(query_str, params=None):
#     """Execute a SQL query with optional parameters using SQLAlchemy."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query_str), params or {})
#             return True, result.fetchall()
#     except Exception as e:
#         logging.error(f"Query failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# def run_query_insert_update(query_str, params=None):
#     """Execute a SQL query with optional parameters."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query_str), params) if params else conn.execute(text(query_str))
#             query_type = query_str.strip().upper().split()[0]
#             if query_type in ("INSERT", "UPDATE", "DELETE"):
#                 conn.commit()
#                 return True, None
#             elif query_type == "SELECT":
#                 return True, result.fetchall()
#             return True, None
#     except Exception as e:
#         logging.error(f"Database error: {e}")
#         return False, str(e)
#     finally:
#         engine.dispose()

# def run_query_to_df(query_str, params=None):
#     """Execute a query and return the result as a Pandas DataFrame."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = pd.read_sql_query(text(query_str), conn, params=params or {})
#             return True, result
#     except Exception as e:
#         logging.error(f"Query to DataFrame failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# def run_query_to_list_of_dicts(query_str, params=None):
#     """Execute a SQL query and return results as a list of dictionaries."""
#     engine = get_engine()
#     try:
#         with engine.connect() as conn:
#             result = pd.read_sql_query(text(query_str), conn, params=params or {})
#             result_list = result.to_dict(orient="records")
#             return True, result_list
#     except Exception as e:
#         logging.error(f"Query to list of dicts failed: {e}")
#         return False, None
#     finally:
#         engine.dispose()

# # Additional functions

# def get_last_docket_id(program_id, country):
#     """Get the highest docket_id for a given program and country."""
#     query = """
#     SELECT MAX(CAST(docket_id AS UNSIGNED)) AS max_docket 
#     FROM international_documents 
#     WHERE program_id = :program_id AND country = :country
#     """
#     success, result = run_query_to_list_of_dicts(query, {'program_id': str(program_id), 'country': country})
#     if success and result and result[0].get('max_docket') is not None:
#         return int(result[0]['max_docket'])
#     return None

# def get_latest_date(program_id, country):
#     """Get the latest date (modified or publish) for a given program and country."""
#     query = """
#     SELECT MAX(GREATEST(
#         IFNULL(STR_TO_DATE(modified_date, '%Y-%m-%d'), '1900-01-01'),
#         IFNULL(STR_TO_DATE(publish_date, '%Y-%m-%d'), '1900-01-01'),
#         IFNULL(STR_TO_DATE(effective_date, '%Y-%m-%d'), '1900-01-01')
#     )) AS latest_date 
#     FROM international_documents 
#     WHERE program_id = :program_id AND country = :country
#     """
#     success, result = run_query_to_df(query, {'program_id': str(program_id), 'country': country})
#     if success and not result.empty:
#         latest = result.iloc[0]['latest_date']
#         return latest if latest and latest != datetime(1900, 1, 1) else None
#     return None

# def check_duplicate(title, url):
#     """Check if a document is a duplicate based on title and URL hash."""
#     if not title or not url:
#         logging.warning("Title or URL is None or empty; cannot check duplicate.")
#         return True  # Skip if invalid input to avoid duplicates
#     doc_hash = md5((title + url).encode()).hexdigest()
#     query = """
#     SELECT COUNT(*) AS count 
#     FROM international_documents 
#     WHERE doc_hash = :doc_hash
#     """
#     success, result = run_query_to_list_of_dicts(query, {'doc_hash': doc_hash})
#     if success and result:
#         return result[0]['count'] > 0
#     return False

# def insert_metadata(metadata):
#     """Insert metadata into the database."""
#     # Ensure all required fields are present
#     required_fields = [
#         'country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format',
#         's3_country_folder', 'in_elastic', 'create_date', 'modified_date'
#     ]
#     for field in required_fields:
#         if field not in metadata or metadata[field] is None:
#             metadata[field] = metadata.get(field, '') if field in ['country', 'docket_id', 'doc_id', 'title', 'url', 'program_id', 'doc_format', 's3_country_folder'] else None
#             if field in ['create_date', 'modified_date']:
#                 metadata[field] = datetime.now()
    
#     # Calculate doc_hash
#     metadata['doc_hash'] = md5((metadata['title'] + metadata['url']).encode()).hexdigest()
    
#     query = """
#     INSERT INTO international_documents (
#         country, docket_id, doc_id, doc_hash, document_type, agency_id, reference, title, url, abstract, 
#         program_id, publish_date, modified_date, effective_date, doc_format, s3_country_folder, 
#         aws_bucket, aws_key, s3_link_url, in_elastic, create_date, modified_date
#     ) VALUES (
#         :country, :docket_id, :doc_id, :doc_hash, :document_type, :agency_id, :reference, :title, :url, :abstract, 
#         :program_id, :publish_date, :modified_date, :effective_date, :doc_format, :s3_country_folder, 
#         :aws_bucket, :aws_key, :s3_link_url, :in_elastic, :create_date, :modified_date
#     )
#     """
#     success, error = run_query_insert_update(query, metadata)
#     if not success:
#         logging.error(f"Failed to insert metadata: {error}")
#     return success

# def update_s3_details(doc_id, aws_key, s3_link_url):
#     """Update S3 details for a document."""
#     query = """
#     UPDATE international_documents 
#     SET aws_key = :aws_key, s3_link_url = :s3_link_url 
#     WHERE doc_id = :doc_id
#     """
#     success, error = run_query_insert_update(query, {'aws_key': aws_key, 's3_link_url': s3_link_url, 'doc_id': doc_id})
#     if not success:
#         logging.error(f"Failed to update S3 details: {error}")
#     return success