import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup

import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.walmart.com/search?q={formatted_keyword}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed to get page {page_number}, status code {response.status_code}")

            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.select_one("script[id='__NEXT_DATA__'][type='application/json']")
            json_data = json.loads(script_tag.text)
            item_list = json_data["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"]

            for item in item_list:
                if item["__typename"] != "Product":
                    continue
                
                name = item.get("name")
                product_id = item["usItemId"]
                if not name:
                    continue
                link = f"https://www.walmart.com/reviews/product/{product_id}"
                price = item["price"]
                sponsored = item["isSponsoredFlag"]
                rating = item["averageRating"]
                
                search_data = {
                    "name": name,
                    "stars": rating,
                    "url": link,
                    "sponsored": sponsored,
                    "price": price,
                    "product_id": product_id
                }            
                print(search_data)                
                
            logger.info(f"Successfully parsed data from: {url}")
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries += 1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")

if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 3
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["laptop"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")