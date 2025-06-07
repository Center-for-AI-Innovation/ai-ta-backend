import os 
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService

from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import pandas as pd
import crossref_commons.retrieval
import shutil
import supabase
from supabase import create_client, Client

# Initialize the Supabase client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_API_KEY")
SUPABASE_CLIENT: Client = create_client(url, key)

LOG = "log.txt"

def extract_article_metadata(search_str: str) -> str:
    """
    Extract article metadata from NAL website.
    Store the metadata in a SQL database.
    """
    print("Extracting article metadata from NAL website...")
    start_time = time.time()
    # get list of articles - 1st page
    search_results = get_search_results(search_str)
    search_time = time.time() 
    print("Time taken to search results: ", search_time - start_time)

    # for each article, go one level deeper to extract DOI
    search_results = extract_doi(search_results, SUPABASE_CLIENT)
    doi_time = time.time()
    print("Time taken to extract DOI and upload metadata: ", doi_time - search_time)

           
    return "Article metadata extracted successfully."

def get_search_results(query):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")  # Required for running as root
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    chrome_options.add_argument("--log-level=3")

    chrome_path = shutil.which("google-chrome")
    print(f"Chrome binary path: {chrome_path}")
    # path = "C:/Users/dabho/.wdm/drivers/chromedriver/win64/127.0.6533.72/chromedriver-win32/chromedriver.exe"
    # driver_service = Service(path)
    #chrome_options.binary_location ="C:/Users/dabho/.wdm/drivers/chromedriver/win64/127.0.6533.72/chromedriver-win32/chromedriver.exe"
    
    #chrome_options.binary_location = "/usr/bin/google-chrome"
    chrome_options.binary_location = "/opt/google/chrome/google-chrome"

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))


    #driver = webdriver.Chrome(service=driver_service, options=chrome_options)
    
    count = 0
    search_results = []
    sleep_time = 0
    while count < 1000:
        try:
            # Construct the URL with the query
            base_url = "https://search.nal.usda.gov/discovery/search"
            params = f"?query=any,contains,{query}&tab=pubag&search_scope=pubag&vid=01NAL_INST:MAIN&offset={count}"
            url = base_url + params
            print("URL: ", url)
            # Load the page
            driver.get(url)
            
            # Wait for the page to load (you may need to adjust the sleep time)
            time.sleep(sleep_time)
            
            # Find the search results
            results = driver.find_elements(By.CLASS_NAME, 'list-item')
            while len(results) == 0 and sleep_time < 30:
                sleep_time += 1
                print("Sleeping for ", sleep_time, " seconds")
                time.sleep(sleep_time)
                results = driver.find_elements(By.CLASS_NAME, 'list-item')

            if len(results) == 0:
                print("No results found after count: ", count)
                break
            
            # Extract the titles and links
            for result in results:
                title_element = result.find_element(By.CLASS_NAME, 'item-title')
                title = title_element.text.strip()
                link = title_element.find_element(By.TAG_NAME, 'a').get_attribute('href')
                search_results.append({'title': title, 'link': link})
            
            
        except Exception as e:
            print(e)
            
        count += 10
    
    driver.quit()
    return search_results


def extract_doi(main_results, supabase_client):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")  # Required for running as root
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    #path = "C:/Users/dabho/.wdm/drivers/chromedriver/win64/127.0.6533.72/chromedriver-win32/chromedriver.exe"
    #driver_service = Service(path)
    chrome_options.binary_location = "/usr/bin/google-chrome"
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

    sleep_time = 0
    for item in main_results:
        link = item['link']
        try:
            start_time = time.time()

            # Load the page
            driver.get(link)
            
            # Wait for the page to load (adjust sleep time if needed)
            time.sleep(sleep_time)
            
            # Find the search results
            results = driver.find_elements(By.ID, 'item-details')
            while not results and sleep_time < 30:
                sleep_time += 5
                print("Sleeping for ", sleep_time, " seconds")
                time.sleep(sleep_time)
                results = driver.find_elements(By.ID, 'item-details')

            if not results:
                item['doi'] = "N/A"
                continue
            
            # Extract the DOI link
            for result in results:
                try:
                    doi_link_element = result.find_element(By.XPATH, './/a[contains(@href, "https://doi.org/")]')
                    doi_link = doi_link_element.get_attribute("href")
                except Exception:
                    doi_link = "N/A"
                item['doi'] = doi_link

            # Extract DOI from the link
            try: 
                doi = doi_link.split("https://doi.org/")[1]
                print("DOI:", doi)
            except Exception:
                continue

            # Get metadata of the article
            item_metadata = get_article_metadata_from_crossref(doi)
            item['doi_number'] = doi
            item['publisher'] = item_metadata.get('publisher', 'N/A')
            item['metadata'] = item_metadata

            if 'license' in item_metadata:
                # Look for TDM license
                for ele in item_metadata['license']:
                    if ele['content-version'] == 'tdm':
                        item['license'] = ele['URL']
                        break
                
                # If no TDM license, look for VOR license
                if 'license' not in item:
                    for ele in item_metadata['license']:
                        if ele['content-version'] == 'vor':
                            item['license'] = ele['URL']
                            break
            
            # Upload to SQL
            response = supabase_client.table("nal_publications").insert(item).execute()
            #print(response)

            end_time = time.time()
            print("Time taken to process 1 article: ", end_time - start_time)

        except Exception as e:
            print(e)
    
    # Close the browser
    driver.quit()

    return "success"


def get_article_metadata_from_crossref(doi: str):
    """
    Get article metadata from Crossref API.
    """
    # Get metadata from Crossref
    metadata = crossref_commons.retrieval.get_publication_as_json(doi)
    print("Metadata: ", metadata)
    
    return metadata

    

        