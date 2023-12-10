import time
import requests
import mysql.connector
import datetime
from yaml import safe_load

class CryptoMonitor:
    def __init__(self, api_url_base, config_path, db_host, db_user, db_password, db_name):
        self.api_url_base = api_url_base
        self.config_path = config_path
        self.db_connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        self.cursor = self.db_connection.cursor()

    def create_table_if_not_exists(self, coin):
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {coin}_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                price_eur DECIMAL(10, 5)
            );
        '''
        self.cursor.execute(create_table_sql)
        self.db_connection.commit()

    def load_config(self):
        with open(self.config_path, "r") as yaml_file:
            config = safe_load(yaml_file)
        return config

    def fetch_crypto_data(self):
        coins = self.load_config().get("COINS")
        api_url = self.api_url_base.replace("<COINS>", coins)
        response = requests.get(api_url)
        data = response.json()

        # Überprüfe den API-Status
        status = data.get("status", None)
        if status and status.get("error_code") == 429:
            print("API Rate Limit überschritten. Bitte warte, bevor du erneut abrufst.")
            time.sleep(180)
            return None
        elif data.get("error") or data == {}:
            print(f"Request has failded: {data.get('error')}")
            return None
        else:
            print(f"[{datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}] Request wurde erfolgreich durchgeführt!")
        return data

    def insert_data_into_database(self, coin, price_eur):
        self.create_table_if_not_exists(coin)
        insert_sql = f"INSERT INTO {coin}_data (price_eur) VALUES (%s)"
        values = (price_eur,)
        self.cursor.execute(insert_sql, values)
        self.db_connection.commit()

    def run(self, interval=6.5):
        while True:
            try:
                data = self.fetch_crypto_data()

                if data is not None:
                    for coin, details in data.items():
                        price_eur = details['eur']
                        self.insert_data_into_database(coin, price_eur)
                        #print(f"Daten für {coin} erfolgreich in die Datenbank eingefügt.")
            except Exception as e:
                print(f"Fehler beim Abrufen und Speichern der Daten: {e}")

            time.sleep(interval)

    def __del__(self):
        self.cursor.close()
        self.db_connection.close()
