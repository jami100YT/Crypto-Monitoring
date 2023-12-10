from crypto_monitor import CryptoMonitor
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv()
    config_path = os.getenv("CONFIG_PATH")
    api_url_base = os.getenv("API_URL_BASE")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")


    crypto_monitor = CryptoMonitor(api_url_base, config_path, db_host, db_user, db_password, db_name)
    crypto_monitor.run()
