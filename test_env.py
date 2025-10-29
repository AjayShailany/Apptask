# from dotenv import load_dotenv
# import os
# load_dotenv(override=True)
# print("DB_USER:", os.getenv('DB_USER'))
# print("DB_PASSWORD:", os.getenv('DB_PASSWORD'))
# print("DB_HOST:", os.getenv('DB_HOST'))
# print("DB_NAME:", os.getenv('DB_NAME'))
# print("DB_PORT:", os.getenv('DB_PORT'))
from sqlalchemy import create_engine
try:
    engine = create_engine("mysql+pymysql://root:ajay@123@localhost:3306/nafdac_db")
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")