import time
import requests
import mysql.connector
from datetime import datetime
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
                price_eur DECIMAL(10, 5),
                market_cap DECIMAL(20, 5),
                volume DECIMAL(20, 5),
                high_24h DECIMAL(10, 5),
                low_24h DECIMAL(10, 5),
                price_change_24h DECIMAL(10, 5),
                price_change_percentage_24h DECIMAL(10, 5),
                market_cap_change_24h DECIMAL(20, 5),
                market_cap_change_percentage_24h DECIMAL(10, 5),
                circulating_supply DECIMAL(20, 5),
                total_supply DECIMAL(20, 5),
                max_supply DECIMAL(20, 5),
                ath DECIMAL(10, 5),
                ath_change_percentage DECIMAL(10, 5),
                atl DECIMAL(10, 5),
                atl_change_percentage DECIMAL(12, 6),
                last_updated TIMESTAMP,
                symbol VARCHAR(255),
                image TEXT
            );
        '''
        self.cursor.execute(create_table_sql)
        self.db_connection.commit()

    def load_config(self):
        with open(self.config_path, "r") as yaml_file:
            config: dict = safe_load(yaml_file)
        return config

    def fetch_crypto_data(self):
        coins = self.load_config().get("COINS")
        api_url = self.api_url_base.replace("<COINS>", coins)
        response = requests.get(api_url)
        data: list[dict] = response.json()
        
        if(type(data) == dict and data.get("status")):
            print(f'error code: {data.get("status").get("error_code")}\nerror_essage: {data.get("status").get("error_message")}')
            if(data.get("status").get("error_message") == 429):
                time.sleep(180)
                return 429
        elif (type(data) == dict and data.get("error")):
            print(data.get("error"))
            return None 
        else:
            print(f"[{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}] Request wurde erfolgreich durchgeführt!")
            return data 


    def insert_data_into_database(self, coin, details):
        self.create_table_if_not_exists(coin)

        # Überprüfung für atl_change_percentage-Wert
        atl_change_percentage = details['atl_change_percentage']
        if atl_change_percentage is not None and isinstance(atl_change_percentage, (int, float)):
            # Begrenzen Sie die Dezimalstellen auf 6
            atl_change_percentage = round(atl_change_percentage, 6)
        else:
            atl_change_percentage = None

        insert_sql = f'''
            INSERT INTO {coin}_data (
                price_eur, market_cap, volume, high_24h, low_24h, 
                price_change_24h, price_change_percentage_24h, 
                market_cap_change_24h, market_cap_change_percentage_24h,
                circulating_supply, total_supply, max_supply,
                ath, ath_change_percentage, atl, atl_change_percentage,
                last_updated, symbol, image
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            details['current_price'], details['market_cap'], details['total_volume'],
            details['high_24h'], details['low_24h'], details['price_change_24h'],
            details['price_change_percentage_24h'], details['market_cap_change_24h'],
            details['market_cap_change_percentage_24h'], details['circulating_supply'],
            details['total_supply'], details['max_supply'], details['ath'],
            details['ath_change_percentage'], details['atl'], details['atl_change_percentage'],
            datetime.strptime(details['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ'), details["symbol"], details["image"]
        )
        self.cursor.execute(insert_sql, values)
        self.db_connection.commit()

    def run(self, interval=11.5): #6.5
        while True:
            try:
                data = self.fetch_crypto_data()

                if data is not None and data != 429:
                    for coin in data:
                        self.insert_data_into_database(coin.get("id"), coin)
                        #print(f"Daten für {coin['name']} erfolgreich in die Datenbank eingefügt.")

                else:
                    time.sleep(180)
            except Exception as e:
                print(f"Fehler beim Abrufen und Speichern der Daten: {e}")

            time.sleep(interval)

    def __del__(self):
        self.cursor.close()
        self.db_connection.close()