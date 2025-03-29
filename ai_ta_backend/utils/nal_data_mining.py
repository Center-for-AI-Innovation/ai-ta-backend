from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service  # Import the Service class
from selenium.webdriver.common.by import By

import os
import time
import json
import pandas as pd
import crossref_commons.retrieval
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor, as_completed


# Initialize the Supabase client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_API_KEY")
SUPABASE_CLIENT: Client = create_client(url, key)

# Set up the Chrome options if needed
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run Chrome in headless mode
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Create a Service object using ChromeDriverManager
SERVICE = Service(ChromeDriverManager().install())

# Initialize the Chrome WebDriver with the service and options
DRIVER = webdriver.Chrome(service=SERVICE, options=options)

NAL_LOG = "nal_datamining_log.txt"

if not os.path.exists(NAL_LOG):
    with open(NAL_LOG, "w") as f:
        f.write("NAL Data Mining Log\n")

def get_search_results(query):
    driver = webdriver.Chrome(service=SERVICE, options=options)
    count = 0
    search_results = []
    sleep_time = 0
    while count < 10000:
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
                #print("Sleeping for ", sleep_time, " seconds")
                time.sleep(sleep_time)
                results = driver.find_elements(By.CLASS_NAME, 'list-item')
            with open(NAL_LOG, 'a') as f:
                f.write(f"sleep time for {query} is {sleep_time}\n")
            if not results or len(results) == 0:
                print("No more results: ", count)
                break
            
            # Extract the titles and links
            for result in results:
                title_element = result.find_element(By.CLASS_NAME, 'item-title')
                title = title_element.text.strip()
                link = title_element.find_element(By.TAG_NAME, 'a').get_attribute('href')
                #yield {'title': title, 'link': link}
                search_results.append({'title': title, 'link': link})
            count += 10
            print("count in get_search_results incremented: ", count)
        except Exception as e:
            print("Error: ", e)
            with open(NAL_LOG, 'a') as f:
                f.write(f"Error in get_search_results 1: {e}\n")

    try:
        with open(NAL_LOG, 'a') as f:
            f.write(f"Query: {query}, Number of Results: {len(search_results)}, Stopped at offset: {count} \n")
    except Exception as e:
        print(e)
        with open(NAL_LOG, 'a') as f:
            f.write(f"Error in get_search_results 2: {e}\n")


    driver.quit()
    print("len of search results: ", len(search_results))
    return search_results


def get_article_metadata_from_crossref(doi: str):
    """
    This function calls the crossref.org API to retrieve the metadata of a journal article.
    """    
    metadata = crossref_commons.retrieval.get_publication_as_json(doi)
    return metadata

def process_item(item, supabase_client):
    driver = webdriver.Chrome(service=SERVICE, options=options)
    sleep_time = 0
    start_time = time.time()
    print("inside process_item()")
    link = item['link']
    try:
        # Load the page
        driver.get(link)

        # Wait for the page to load (adjust sleep time if needed)
        time.sleep(sleep_time)

        # Find the search results
        results = driver.find_elements(By.ID, 'item-details')
        while not results and sleep_time < 30:
            sleep_time += 1
            #print("Sleeping for ", sleep_time, " seconds")
            time.sleep(sleep_time)
            results = driver.find_elements(By.ID, 'item-details')

        if not results:
            item['doi'] = "N/A"
            return item
        #print("found some results.")
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
        except Exception:
            return item
        #print("extracted DOI from link")
        # Get metadata of the article
        item_metadata = get_article_metadata_from_crossref(doi)
        item['doi_number'] = doi
        #print("got metadata")
        # check if doi number already exists in SQL
        sql_response = SUPABASE_CLIENT.table("nal_publications").select("doi").eq("doi_number", doi).execute()
        if len(sql_response.data) > 0:
            with open(NAL_LOG, 'a') as f:
                f.write(f"DOI {doi} already present in Supabase.\n")
            return item
        #print("performed SQL check")
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
        #print("before upload")
        # Upload to SQL
        response = SUPABASE_CLIENT.table("nal_publications").insert(item).execute()
        #print("after upload")
        # Optionally log response or errors
    except Exception as e:
        print(f"Error processing {link}: {e}")
        with open(NAL_LOG, 'a') as f:
            f.write(f"Error in process_item: {e}\n")
    finally:
        driver.quit()
    
    end_time = time.time()
    print(f"Time taken to extract DOI for article: {end_time - start_time}")
    return item

def extractDOI(main_results, supabase_client, max_workers=10):
    process_start_time = time.time()
    print("inside extractDOI()")
    try:
        for item in main_results:
            try:
                process_item(item, supabase_client)
            except Exception as e:
                print("Error: ", e)
                continue

        process_end_time = time.time()
        print(f"Total time for processing all results: {process_end_time - process_start_time}")
    except Exception as e:
        with open(NAL_LOG, 'a') as f:
            f.write(f"Error in  extractDOI: {e}\n")


def main():
    # read keywords from CSV file
    KEYWORDS = []
    count = 0
    countline = 0
    keywords_file = "ai_ta_backend/keywords.txt"
    with open(keywords_file, 'r', encoding='utf-8') as f:
        for line in f:
            count += 1
            line = line.strip()
            if line:
                countline += 1
                KEYWORDS.append(line)
    curr_keywords = KEYWORDS[:100]
    print(len(KEYWORDS))
    print("count: ", count)
    print(countline)

    process_start_time = time.time()
    count = 0
    # Process the unique keywords
    for keyword in curr_keywords:
        keyword = keyword.replace(" ", "%20")
        count += 1
        print(f"Processing keyword {count}/{len(curr_keywords)}")

        with open(NAL_LOG, "a") as f:
            f.write(f"Processing keyword: {keyword}\n")
        keyword_start_time = time.time()

        print("Searching for papers on: ", keyword)
        main_results = get_search_results(keyword)

        print("len of returned results: ", len(main_results))

        # for result_row in get_search_results(keyword):
        #     print("result row: ", result_row)

        search_time = time.time() 
        with open(NAL_LOG, "a") as f:
            f.write(f"Search time: {search_time - keyword_start_time}\n")

        doi_start_time = time.time()
        extractDOI(main_results, SUPABASE_CLIENT)
        doi_end_time = time.time()
        with open(NAL_LOG, "a") as f:
            f.write(f"DOI time: {doi_start_time - doi_end_time}\n")

        print("Done searching for papers on: ", keyword)
        keyword_end_time = time.time()

        with open(NAL_LOG, "a") as f:
            f.write(f"Total time for keyword: {keyword_end_time - keyword_start_time}\n")
            f.write(f"--------------------------------------------------\n")

    process_end_time = time.time()
    with open(NAL_LOG, "a") as f:
            f.write(f"Total time taken for 40 keywords: {process_end_time-process_start_time}\n")

if __name__ == "__main__":
    main()