from dotenv import load_dotenv
import os
from urllib.parse import quote
import pymysql  # Add this import for database connections
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LeximGPTDb:
    def __init__(self):
        load_dotenv(override=True)
        self.db_name = os.getenv("DB_NAME")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT", "3306")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        
        if not all([self.db_user, self.db_password, self.db_host, self.db_name]):
            raise ValueError("Database credentials not found in environment variables")
        
        # URL-encode the password to handle special characters
        db_password_encoded = quote(self.db_password, safe='')
        self.db_connect_str = f'mysql+pymysql://{self.db_user}:{db_password_encoded}@{self.db_host}:{self.db_port}/{self.db_name}'

    def get_connection_str(self):
        return self.db_connect_str

    def run_query(self, query, params=None):
        """Execute a SQL query with optional parameters."""
        try:
            connection = pymysql.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                port=int(self.db_port),
                database=self.db_name
            )
            cursor = connection.cursor()
            cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            connection.commit()
            return True
        except Exception as e:
            logging.error(f"Database query failed: {e}")
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals() and connection.open:
                connection.close()

ldb = LeximGPTDb()

# from dotenv import load_dotenv
# import os
# from urllib.parse import quote


# class LeximGPTDb:
#     def __init__(self):
#         load_dotenv(override=True)
#         db_name = os.getenv("DB_NAME")
#         db_host = os.getenv("DB_HOST")
#         db_port = os.getenv("DB_PORT", "3306")
#         db_user = os.getenv("DB_USER")
#         db_password = os.getenv("DB_PASSWORD")
        
#         if not all([db_user, db_password, db_host, db_name]):
#             raise ValueError("Database credentials not found in environment variables")
        
#         # URL-encode the password to handle special characters
#         db_password_encoded = quote(db_password, safe='')
#         self.db_connect_str = f'mysql+pymysql://{db_user}:{db_password_encoded}@{db_host}:{db_port}/{db_name}'

#     def get_connection_str(self):
#         return self.db_connect_str

# ldb = LeximGPTDb()