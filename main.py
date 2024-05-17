import os
import json
import requests
import uuid
import pytz
from datetime import datetime   
from dotenv import load_dotenv

from db.mongodb_service import MongoDBService

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
CONFIG_FILE = 'config.json'


def fetch_json(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse and return the JSON content
        return response.json()
    
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except requests.exceptions.ConnectionError as conn_err:
        print(f'Connection error occurred: {conn_err}')
    except requests.exceptions.Timeout as timeout_err:
        print(f'Timeout error occurred: {timeout_err}')
    except requests.exceptions.RequestException as req_err:
        print(f'An error occurred: {req_err}')
    except ValueError as json_err:
        print(f'JSON decode error: {json_err}')

if __name__=="__main__":

    mongodb = MongoDBService(MONGODB_URI, DB_NAME) 

    utc = pytz.UTC

    if not mongodb.collection_exists(COLLECTION_NAME):
        mongodb.create_collection(collection_name=COLLECTION_NAME, unique_indexes=[[("symbol", 1), ("exchange", 1)], ("id")])

    with open(CONFIG_FILE) as f:
        configs = json.load(f)

        for config in configs:
            exchange = str(config["exchange"]).upper()
            country = str(config["country"]).upper()
            src_url = config["src_url"]
            
            print(f"fetching symbols from exchange: {exchange}, src_url: {src_url}")
            symbols_json = fetch_json(src_url)

            symbols_documents = []
            if symbols_json:
                for symbol_json in symbols_json:
                    symbol_document = {
                        "id": str(uuid.uuid4()),
                        "symbol": str(symbol_json["symbol"]).upper(),
                        "name": symbol_json["name"],
                        "country": country,
                        "exchange": exchange,
                        "industry": symbol_json["industry"],
                        "sector": symbol_json["sector"],
                        "date_created": datetime.now(utc)
                    }

                    symbols_documents.append(symbol_document)

            insertion_results = mongodb.insert_many(collection=COLLECTION_NAME, documents=symbols_documents, ordered=False)
            print(f"insertion results: {insertion_results.inserted_ids}")

                