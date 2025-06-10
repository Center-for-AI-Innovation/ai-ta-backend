import os
from concurrent.futures import as_completed

import requests
from dotenv import load_dotenv
from supabase import create_client

from ai_ta_backend.executors.thread_pool_executor import ThreadPoolExecutorAdapter

load_dotenv()


def send_request(webcrawl_url, payload):
  response = requests.post(webcrawl_url, json=payload)
  return response.json()


def webscrape_documents(project_name: str):
  print(f"Scraping documents for project: {project_name}")

  # create Supabase client
  supabase_url = os.getenv("SUPABASE_URL")
  supabase_key = os.getenv("SUPABASE_API_KEY")
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
          "courseName": project_name
      }
  }

  tasks = []
  count = 0
  batch_size = 10

  processed_file_name = f"processed_urls_{''.join(e if e.isalnum() else '_' for e in project_name.lower())}.txt"
  if not os.path.exists(processed_file_name):
    open(processed_file_name, 'w').close()

  print(f"Processed file name: {processed_file_name}")

  with ThreadPoolExecutorAdapter(max_workers=batch_size) as executor:
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

      with open(processed_file_name, 'a') as file:
        file.write(base_url + '\n')

      tasks.append(executor.submit(send_request, webcrawl_url, payload.copy()))
      count += 1

      if count % batch_size == 0:
        for future in as_completed(tasks):
          response = future.result()
          print("Response from crawl: ", response)
        tasks = []
        #return "Webscrape done."

    # Process remaining tasks
    for future in as_completed(tasks):
      response = future.result()
      print("Response from crawl: ", response)

  # if os.path.exists(processed_file_name):
  #     os.remove(processed_file_name)
  #     print(f"Removed file: {processed_file_name}")

  return "Webscrape done."


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python rerun_webcrawl_for_project.py <project_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    result = webscrape_documents(project_name)
    print(result)
