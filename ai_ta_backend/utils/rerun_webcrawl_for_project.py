import os
from concurrent.futures import as_completed, ThreadPoolExecutor
import argparse

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


def send_request(webcrawl_url, payload):
  response = requests.post(webcrawl_url, json=payload)
  return response.json()


def webscrape_documents(project_name: str, source_url=None, source_key=None, destination_url=None, destination_key=None):
  print(f"Scraping documents for project: {project_name}")

  # create Supabase client (source)
  supabase_url = source_url or os.getenv("SUPABASE_URL")
  supabase_key = source_key or os.getenv("SUPABASE_API_KEY") or os.getenv("SUPABASE_KEY")
  if not supabase_url or not supabase_key:
    print("Error: SUPABASE_URL and SUPABASE_API_KEY (or SUPABASE_KEY) environment variables must be set or provided as arguments.")
    return
  supabase_client = create_client(supabase_url, supabase_key)

  # use RPC to get unique base_urls
  response = supabase_client.rpc("get_base_url_with_doc_groups", {"p_course_name": project_name}).execute()
  print("Supabase RPC response:", response)
  base_urls = response.data
  if not base_urls:
    print("No base URLs found or Supabase RPC failed.")
    print("Supabase response:", response)
    return
  print(f"Total base_urls: {len(base_urls)}")

  if not response.data:
    print("Supabase error:", getattr(response, 'error', 'No error attribute'))
    print("Supabase raw response:", response)
    return

  # Add extra URLs with their associated document groups
  extra_urls_with_groups = {
      "https://nature.berkeley.edu/cooperative-extension": ["UC Berkeley"],
      "https://caes.ucdavis.edu/outreach/ce": ["UC Davis"],
      "https://caes.ucdavis.edu": ["UC Davis"],
      "https://www.aces.edu/": ["Alabama Cooperative Extension System"],
      "https://synthesis.yale.edu/products-publications": ["Yale University"],
      "https://www.canr.msu.edu/tribal_education/": ["Michigan State University"],
      "https://www.wetcc.edu/extension/": ["White Earth Tribal and Community College"],
      "https://www.ecolibrium3.org/fond-du-lac-tribal-and-community-college-environmental-institute/": ["Fond du Lac Tribal and Community College"],
      "https://tribalextension.org/project/leech-lake/": ["Leech Lake Tribal College"],
      "https://www.montana.edu/extension/flatheadres/": ["Montana State University"],
      "https://bfcc.edu/post/USDA-Extension": ["Blackfeet Community College"],
      "https://www.fpcc.edu/special-projects/ag-department/extension-services/": ["Fort Peck Community College"],
      "https://extension.skc.edu/": ["Salish Kootenai College"],
      "https://www.littlepriest.edu/lptc-equity-extension/": ["Little Priest Tribal College"],
      "https://nativecoalition.unl.edu/": ["University of Nebraska–Lincoln"],
      "https://iaia.edu/outreach/land-grant/": ["Institute of American Indian Arts"],
      "https://tribalextension.nmsu.edu/": ["New Mexico State University"],
      "https://www.littlehoop.edu/community/land-grant/": ["Cankdeska Cikana Community College"],
      "https://extension.sdstate.edu/": ["South Dakota State University"],
      "https://extension.wsu.edu/pendoreille/kalispel-tribal-extension-2/": ["Washington State University"],
      "http://www.comfsm.fm/myShark/news/item=3219/mod=10:43:04": ["College of Micronesia-FSM"],
      "https://blogs.ifas.ufl.edu/global/category/agriculture/": ["University of Florida"],
  }

  for url, groups in extra_urls_with_groups.items():
      if url not in base_urls:
          base_urls[url] = groups

  # Output all URLs to a text file
  all_urls_file = "all_urls_to_scrape.txt"
  with open(all_urls_file, 'w') as f:
      for url in base_urls:
          f.write(url + '\n')
  print(f"All URLs to be scraped written to: {all_urls_file}")

  webcrawl_url = "http://localhost:3000/crawl"

  payload = {
      "params": {
          "url": "",
          "scrapeStrategy": "same-hostname",
          "maxPagesToCrawl": 15000,
          "maxTokens": 2000000,
          "courseName": project_name,
          "destinationSupabaseUrl": destination_url,
          "destinationSupabaseKey": destination_key,
      }
  }

  tasks = []
  count = 0
  batch_size = 10

  processed_file_name = f"processed_urls_{''.join(e if e.isalnum() else '_' for e in project_name.lower())}.txt"
  if not os.path.exists(processed_file_name):
    open(processed_file_name, 'w').close()

  print(f"Processed file name: {processed_file_name}")

  with ThreadPoolExecutor(max_workers=batch_size) as executor:
    for base_url in base_urls:
      document_groups = base_urls[base_url]
      payload["params"]["url"] = base_url
      if not document_groups:
        continue

      # Read the file process_urls.txt and skip all the URLs mentioned there
      with open(processed_file_name, 'r') as file:
        skip_urls = set(line.strip() for line in file)

      if base_url in skip_urls:
        print(f"Skipping URL: {base_url}")
        continue

      payload["params"]["documentGroups"] = base_urls[base_url]
      print("Payload: ", payload)

      tasks.append(executor.submit(send_request, webcrawl_url, payload.copy()))
      count += 1

      if count % batch_size == 0:
        for future in as_completed(tasks):
          response = future.result()
          print("Response from crawl: ", response)
          # Only write to processed file after a successful crawl
          if response and (isinstance(response, dict) or isinstance(response, str)):
            with open(processed_file_name, 'a') as file:
              file.write(payload["params"]["url"] + '\n')
        tasks = []
        #return "Webscrape done."

    # Process remaining tasks
    for future in as_completed(tasks):
      response = future.result()
      print("Response from crawl: ", response)
      # Only write to processed file after a successful crawl
      if response and (isinstance(response, dict) or isinstance(response, str)):
        with open(processed_file_name, 'a') as file:
          file.write(payload["params"]["url"] + '\n')

  # if os.path.exists(processed_file_name):
  #     os.remove(processed_file_name)
  #     print(f"Removed file: {processed_file_name}")

  return "Webscrape done."


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Webscrape documents for a project using flexible Supabase credentials.")
    parser.add_argument("project_name", help="Project/course name to scrape")
    parser.add_argument("--source-url", type=str, help="Source Supabase URL (overrides env SUPABASE_URL)")
    parser.add_argument("--source-key", type=str, help="Source Supabase Key (overrides env SUPABASE_API_KEY/SUPABASE_KEY)")
    parser.add_argument("--destination-url", type=str, help="Destination Supabase URL (overrides env CROPWIZARD_SUPABASE_URL)")
    parser.add_argument("--destination-key", type=str, help="Destination Supabase Key (overrides env CROPWIZARD_SUPABASE_KEY)")
    args = parser.parse_args()

    # Set source credentials: prefer CLI args, then SUPABASE_*
    source_url = args.source_url or os.environ.get("SUPABASE_URL")
    source_key = args.source_key or os.environ.get("SUPABASE_API_KEY") or os.environ.get("SUPABASE_KEY")
    # Set destination credentials: prefer CLI args, then CROPWIZARD_*
    destination_url = args.destination_url or os.environ.get("CROPWIZARD_SUPABASE_URL")
    destination_key = args.destination_key or os.environ.get("CROPWIZARD_SUPABASE_KEY")

    # Warn if source and destination credentials are the same
    if (source_url and destination_url and source_url == destination_url \
        and source_key and destination_key and source_key == destination_key):
        print("Warning: Source and destination Supabase credentials are identical. You are scraping within the same database/account.")

    result = webscrape_documents(args.project_name, source_url, source_key, destination_url, destination_key)
    print(result)
