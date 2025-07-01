import asyncio
import concurrent.futures
import ftplib
import gzip
import json
import os
import shutil
import tarfile
import threading
import time
import xml.etree.ElementTree as ET
from functools import partial
from multiprocessing import Manager
from urllib.parse import urlparse

import pandas as pd
import requests
import supabase
from minio import Minio
from posthog import Posthog

# POSTHOG = Posthog(sync_mode=False, project_api_key=os.environ['POSTHOG_API_KEY'], host="https://app.posthog.com")

# SUPBASE_CLIENT = supabase.create_client(  # type: ignore
#     supabase_url=os.getenv('SUPABASE_URL'),  # type: ignore
#     supabase_key=os.getenv('SUPABASE_API_KEY')  # type: ignore
# )

# MINIO_CLIENT = Minio(os.environ['MINIO_ENDPOINT'],
#                      access_key=os.environ['MINIO_ACCESS_KEY'],
#                      secret_key=os.environ['MINIO_SECRET'],
#                      secure=True)


def extractPubmedData():
  """
  Main function to extract metadata and articles from the PubMed baseline folder.
  """
  global POSTHOG, SUPABASE_CLIENT, MINIO_CLIENT
    
  # Initialize only if not already initialized
  if 'POSTHOG' not in globals():
        POSTHOG = Posthog(sync_mode=False, 
                         project_api_key=os.environ['POSTHOG_API_KEY'], 
                         host="https://app.posthog.com")

  if 'SUPABASE_CLIENT' not in globals():
        SUPABASE_CLIENT = supabase.create_client(  # type: ignore
            supabase_url=os.getenv('SUPABASE_URL'),  # type: ignore
            supabase_key=os.getenv('SUPABASE_API_KEY')  # type: ignore
        )

  if 'MINIO_CLIENT' not in globals():
        MINIO_CLIENT = Minio(os.environ['MINIO_ENDPOINT'],
                            access_key=os.environ['MINIO_ACCESS_KEY'],
                            secret_key=os.environ['MINIO_SECRET'],
                            secure=True)

  start_time = time.monotonic()

  ftp_address = "ftp.ncbi.nlm.nih.gov"
  #ftp_path = "pubmed/baseline"
  ftp_path = "pubmed/updatefiles"
  file_list = getFileList(ftp_address, ftp_path, ".gz")
  print("Total files: ", len(file_list))

  # with concurrent.futures.ProcessPoolExecutor() as executor:
  #     futures = [executor.submit(processPubmedXML, file, ftp_address, ftp_path) for file in file_list[131:133]]
  #     for future in concurrent.futures.as_completed(futures):
  #         try:
  #             future.result()
  #         except Exception as e:
  #             print("Error processing file: ", e)

  files_to_process = getFilesToProcess(file_list)

  for file in files_to_process:
    status = processPubmedXML(file, ftp_address, ftp_path)
    print("Status: ", status)

  end_time = time.monotonic()

  POSTHOG.capture(distinct_id="pubmed_extraction",
                  event="total_pubmed_extraction_runtime",
                  properties={
                      "total_runtime": end_time - start_time,
                  })

  return "success"


def getFilesToProcess(file_list: list):
  last_processed_response = SUPABASE_CLIENT.table("pubmed_daily_update").select("*").order(
      "created_at", desc=True).limit(1).execute()  # type: ignore
  last_processed_file = last_processed_response.data[0]['last_xml_file']
  print("Last processed file: ", last_processed_file)
  files_to_process = []

  for file in file_list:
    if file == last_processed_file:
      break
    files_to_process.append(file)

  return files_to_process


def processPubmedXML(file: str, ftp_address: str, ftp_path: str):
  """
    Main function to extract metadata and articles from the PubMed baseline folder.
    """
  start_time = time.monotonic()
  try:
    print("Processing file: ", file)
    gz_filepath = downloadXML(ftp_address, ftp_path, file, "pubmed")
    gz_file_download_time = time.time()

    # extract the XML file
    if not gz_filepath:
      return "failure"
    xml_filepath = extractXMLFile(gz_filepath)

    xml_id = xml_filepath[7:-4].replace(".", "_")
    destination_dir = xml_id + "_papers"
    csv_filepath = xml_id + "_metadata.csv"
    error_log = xml_id + "_errors.txt"

    for i, metadata in enumerate(extractMetadataFromXML(xml_filepath, destination_dir, error_log)):
      metadata_extract_start_time = time.time()

      batch_dir = os.path.join(destination_dir, f"batch_{i+1}")
      os.makedirs(batch_dir, exist_ok=True)

      # find PMC ID and DOI for all articles
      metadata_with_ids = getArticleIDs(metadata, error_log)
      metadata_update_time = time.time()
      print("Time taken to get PMC ID and DOI for 100 articles: ",
            round(metadata_update_time - metadata_extract_start_time, 2), "seconds")

      # download the articles
      complete_metadata = downloadArticles(metadata_with_ids, batch_dir, error_log)
      print("Time taken to download 100 articles: ", round(time.time() - metadata_update_time, 2), "seconds")

      # store metadata in csv file
      df = pd.DataFrame(complete_metadata)

      # add a column for the XML file path
      df['xml_filename'] = os.path.basename(xml_filepath)

      if os.path.isfile(csv_filepath):
        df.to_csv(csv_filepath, mode='a', header=False, index=False)
      else:
        df.to_csv(csv_filepath, index=False)

      before_upload = time.time()
      # upload current batch to minio
      print(f"Starting async upload for batch {i+1}...")
      #asyncio.run(uploadToStorage(batch_dir, error_log))
      with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(uploadToStorage, batch_dir, error_log)
      after_upload = time.time()

      print("Time elapsed between upload call: ", after_upload - before_upload)
      POSTHOG.capture(distinct_id="pubmed_extraction",
                      event="uploadToMinio",
                      properties={
                          "description": "upload files to minio",
                          "total_runtime": after_upload - before_upload,
                      })

      print("Time taken to download 100 articles: ", round(time.time() - metadata_extract_start_time, 2), "seconds")

    post_download_time_1 = time.monotonic()

    # upload metadata to SQL DB
    df = pd.read_csv(csv_filepath)
    complete_metadata = df.to_dict('records')
    final_metadata = []
    unique_pmids = []
    for item in complete_metadata:
      for key, value in item.items():
        if pd.isna(value):  # Or: math.isnan(value)
          item[key] = None

      # check for duplicates
      if item['pmid'] not in unique_pmids:
        final_metadata.append(item)
        unique_pmids.append(item['pmid'])
    print("Final metadata: ", len(final_metadata))

    try:
      response = SUPABASE_CLIENT.table("publications").upsert(final_metadata).execute()  # type: ignore
      print("Uploaded metadata to SQL DB.")
    except Exception as e:
      print("Error in uploading to Supabase: ", e)
      # log the supabase error
      with open(error_log, 'a') as f:
        f.write("Error in Supabase upsert: " + str(e) + "\n")

    post_download_time_2 = time.monotonic()

    POSTHOG.capture(distinct_id="pubmed_extraction",
                    event="uploadToSupabase",
                    properties={
                        "description": "Process and upload metadata file to Supabase",
                        "total_runtime": post_download_time_2 - post_download_time_1,
                    })

    # upload txt articles to bucket
    # print("Uploading articles to storage...")
    # #destination_dir = "/home/avd6/chatbotai/asmita/ai-ta-backend/papers"
    # #error_log = "all_errors.txt"
    # #asyncio.run(uploadToStorage(destination_dir, error_log))
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     future = executor.submit(uploadToStorage, destination_dir, error_log)
    #     print("Upload started asynchronously.")

    post_download_time_3 = time.monotonic()

    # delete files
    # os.remove(csv_filepath)
    # os.remove(xml_filepath)
    # os.remove(gz_filepath)
    print("Finished file: ", file)

  except Exception as e:
    print("Error processing XML file: ", e)
    with open("errors.txt", 'a') as f:
      f.write("Error in proccesing XML file: " + file + str(e) + "\n")

  end_time = time.monotonic()

  POSTHOG.capture(distinct_id="pubmed_extraction",
                  event="processPubmedXML",
                  properties={
                      "total_runtime": end_time - start_time,
                  })


def downloadXML(ftp_address: str, ftp_path: str, file: str, local_dir: str):
  """
    Downloads a .gz XML file from the FTP baseline folder and stores it in the local directory.
    Args:
        ftp_address: FTP server address.
        ftp_path: Path to the FTP folder.
        file: File to download.
        local_dir: Local directory to store the downloaded file.
    Returns:
        local_filepath: Path to the downloaded file.
    """
  try:
    # create local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)

    # connect to the FTP server
    ftp = ftplib.FTP(ftp_address)
    ftp.login()
    ftp.cwd(ftp_path)

    local_filepath = os.path.join(local_dir, file)
    with open(local_filepath, 'wb') as f:
      ftp.retrbinary('RETR ' + file, f.write)

    # print(f"Downloaded {file} to {local_filepath}")

    ftp.quit()
    return local_filepath
  except Exception as e:
    print("Error downloading file: ", e)
    return None


def getFileList(ftp_address: str, ftp_path: str, extension: str = ".gz"):
  """
    Returns a list of .gz files in the FTP baseline folder.
    Args:
        ftp_address: FTP server address.
        ftp_path: Path to the FTP folder.
        extension: File extension to filter for.
    Returns:
        gz_files: List of .gz files in the FTP folder.
    """
  try:
    # connect to the FTP server
    ftp = ftplib.FTP(ftp_address)
    ftp.login()

    # Change directory to the specified path
    ftp.cwd(ftp_path)

    # Get list of file entries
    file_listing = ftp.nlst()

    ftp.quit()

    # Filter for files with the specified extension
    gz_files = [entry for entry in file_listing if entry.endswith(extension)]
    gz_files.sort(reverse=True)
    print(f"Found {len(gz_files)} files on {ftp_address}/{ftp_path}")

    return gz_files
  except Exception as e:
    print("Error getting file list: ", e)
    return []


def extractXMLFile(gz_filepath: str):
  """
    Extracts the XML file from the .gz file.
    Args:
        gz_filepath: Path to the .gz file.
    Returns:
        xml_filepath: Path to the extracted XML file.
    """
  start_time = time.monotonic()
  try:
    # print("Downloaded .gz file path: ", gz_filepath)
    xml_filepath = gz_filepath.replace(".gz", "")
    with gzip.open(gz_filepath, 'rb') as f_in:
      with open(xml_filepath, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    POSTHOG.capture(distinct_id="pubmed_extraction",
                    event="extractXMLFile",
                    properties={
                        "total_runtime": time.monotonic() - start_time,
                    })

    return xml_filepath
  except Exception as e:
    print("Error extracting XML file: ", e)
    return None


def extractMetadataFromXML(xml_filepath: str, dir: str, error_file: str):
  """
    Extracts article details from the XML file and stores it in a dictionary.
    Details extracted: PMID, PMCID, DOI, ISSN, journal title, article title,
    last revised date, published date, abstract.
    Args:
        xml_filepath: Path to the XML file.
    Returns:
        metadata: List of dictionaries containing metadata for each article.
    """
  print("inside extractMetadataFromXML()")
  start_time = time.monotonic()
  try:
    # create a directory to store papers
    os.makedirs(dir, exist_ok=True)

    tree = ET.parse(xml_filepath)
    root = tree.getroot()
    metadata = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
      futures = []
      article_items = list(item for item in root.iter('PubmedArticle'))  # Convert generator to list

      for item in article_items:
        future = executor.submit(processArticleItem, item, dir, error_file)
        article_data = future.result()
        metadata.append(article_data)

        if len(metadata) == 100:
          # print("collected 100 articles")
          yield metadata
          metadata = []  # reset metadata for next batch

    if metadata:
      yield metadata

    # print("Metadata extraction complete.")
    POSTHOG.capture(distinct_id="pubmed_extraction",
                    event="extractMetadataFromXML",
                    properties={
                        "no_of_articles": 100,
                        "total_runtime": time.monotonic() - start_time,
                    })
  except Exception as e:
    #print("Error extracting metadata: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in main metadata extraction function: " + str(e) + "\n")
    return []


def processArticleItem(item: ET.Element, directory: str, error_file: str):
  """
    Extracts article details from a single PubmedArticle XML element. This is used in the process pool executor.
    Args:
        item: PubmedArticle XML element.
    Returns:
        article_data: Dictionary containing metadata for the article.
    """
  try:
    article_data = {}

    medline_citation = item.find('MedlineCitation')
    article = medline_citation.find('Article')
    journal = article.find('Journal')
    issue = journal.find('JournalIssue')

    if medline_citation.find('PMID') is not None:
      article_data['pmid'] = medline_citation.find('PMID').text
      article_data['pmcid'] = None
      article_data['doi'] = None
    else:
      return article_data

    if journal.find('ISSN') is not None:
      article_data['issn'] = journal.find('ISSN').text
    else:
      article_data['issn'] = None

    if journal.find('Title') is not None:
      article_data['journal_title'] = journal.find('Title').text
    else:
      article_data['journal_title'] = None

    # some articles don't have an article title
    article_title = article.find('ArticleTitle')
    if article_title is not None and article_title.text is not None:
      article_data['article_title'] = article_title.text.replace('[', '').replace(']', '')
    else:
      article_data['article_title'] = None

    article_data[
        'last_revised'] = f"{medline_citation.find('DateRevised/Year').text}-{medline_citation.find('DateRevised/Month').text}-{medline_citation.find('DateRevised/Day').text}"

    # some articles don't have all fields present for publication date
    if issue.find('PubDate/Year') is not None and issue.find('PubDate/Month') is not None and issue.find(
        'PubDate/Day') is not None:
      article_data[
          'published'] = f"{issue.find('PubDate/Year').text}-{issue.find('PubDate/Month').text}-{issue.find('PubDate/Day').text}"
    elif issue.find('PubDate/Year') is not None and issue.find('PubDate/Month') is not None:
      article_data['published'] = f"{issue.find('PubDate/Year').text}-{issue.find('PubDate/Month').text}-01"
    elif issue.find('PubDate/Year') is not None:
      article_data['published'] = f"{issue.find('PubDate/Year').text}-01-01"
    else:
      article_data['published'] = None

    # extract and store abstract in a text file
    abstract = article.find('Abstract')
    abstract_filename = None
    if abstract is not None:
      abstract_text = ""
      for abstract_text_element in abstract.iter('AbstractText'):
        # if labels (objective, methods, etc.) are present, add them to the text (e.g. "OBJECTIVE: ")
        if abstract_text_element.attrib.get('Label') is not None:
          abstract_text += abstract_text_element.attrib.get('Label') + ": "
        if abstract_text_element.text is not None:
          abstract_text += abstract_text_element.text + "\n"

      # save abstract to a text file
      abstract_filename = directory + "/" + article_data['pmid'] + ".txt"
      with open(abstract_filename, 'w') as f:
        if article_data['journal_title']:
          f.write("Journal title: " + article_data['journal_title'] + "\n\n")
        if article_data['article_title']:
          f.write("Article title: " + article_data['article_title'] + "\n\n")
        f.write("Abstract: " + abstract_text)

    # some articles are listed, but not released yet. Adding fields for such articles to maintain uniformity.
    article_data['live'] = True
    article_data['release_date'] = None
    article_data['license'] = None
    article_data['pubmed_ftp_link'] = None
    article_data['filepath'] = abstract_filename

    return article_data
  except Exception as e:
    with open(error_file, 'a') as f:
      f.write("Error in metadata extraction subprocess for PMID " + article_data['pmid'] + ": " + str(e) + "\n")
    return {'error': str(e)}


def getArticleIDs(metadata: list, error_file: str):
  """
    Uses the PubMed ID converter API to get PMCID and DOI for each article.
    Queries the API in batches of 200 articles at a time.
    Also updates the metadata with the release date and live status - some articles are yet to be released.
    Args:
        metadata: List of dictionaries containing metadata for each article.
    Returns:
        metadata: Updated metadata with PMCID, DOI, release date, and live status information.
    """
  #   print("In getArticleIDs()")

  start_time = time.monotonic()

  base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
  app_details = "?tool=ncsa_uiuc&email=caiincsa@gmail.com&format=json"

  batch_size = 200  # maximum number of articles API can process in one request

  for i in range(0, len(metadata), batch_size):
    batch = metadata[i:i + batch_size]
    ids = ",".join([article['pmid'] for article in batch])
    try:
      response = requests.get(base_url + app_details + "&ids=" + ids)
      data = response.json()
      records = data['records']

      # PARALLELIZE THIS FOR LOOP - UPDATES ADDITIONAL FIELDS FOR ALL ARTICLES AT ONCE
      with Manager() as manager:
        shared_metadata = manager.dict()  # Use a shared dictionary
        with concurrent.futures.ProcessPoolExecutor() as executor:
          futures = {executor.submit(updateArticleMetadata, shared_metadata, record): record for record in records}
          concurrent.futures.wait(futures)
          for future in concurrent.futures.as_completed(futures):
            record = futures[future]
            try:
              future.result()
            except Exception as exc:
              print('%r generated an exception: %s' % (record, exc))
              with open(error_file, 'a') as f:
                f.write(f"Record: {record}\t")
                f.write(f"Exception: {type(exc).__name__} - {exc}\n")

        # Update original metadata after loop
        for article in metadata:
          if article['pmid'] in shared_metadata:
            # print("Shared metadata: ", shared_metadata[article['pmid']])
            if 'errmsg' in shared_metadata[article['pmid']]:
              article['live'] = False
            else:
              article['pmcid'] = shared_metadata[article['pmid']]['pmcid']
              article['doi'] = shared_metadata[article['pmid']]['doi']
              article['live'] = shared_metadata[article['pmid']]['live']
              article['release_date'] = shared_metadata[article['pmid']]['release_date']
            #print("Updated metadata: ", article)
    except Exception as e:
      #print("Error: ", e)
      with open(error_file, 'a') as f:
        f.write("Error in getArticleIds(): " + str(e) + "\n")
  #print("Length of metadata after ID conversion: ", len(metadata))

  POSTHOG.capture(distinct_id="pubmed_extraction",
                  event="getArticleIDs",
                  properties={
                      "description": "Converting PMIDs to PMCIDs and update the ID in main metadata",
                      "no_of_articles": 100,
                      "total_runtime": time.monotonic() - start_time,
                  })
  return metadata


def updateArticleMetadata(shared_metadata, record):
  """
    Updates metadata with PMCID, DOI, release date, and live status information for given article.
    Used within getArticleIDs() function.
    """
  if 'errmsg' in record:
    #print("Error: ", record['errmsg'])
    shared_metadata[record['pmid']] = {
        **record,  # Create a copy with record data
        'live': False
    }
  else:
    # Update shared dictionary with pmid as key and updated article data as value
    shared_metadata[record['pmid']] = {
        **record,  # Create a copy with record data
        'pmcid': record['pmcid'],
        'doi': record.get('doi', ''),
        'live': False if 'live' in record and record['live'] == "false" else True,
        'release_date': record['release-date'] if 'release-date' in record else None,
    }

  # POSTHOG.capture(distinct_id = "pubmed_extraction",
  #     event_name = "updateArticleMetadata",
  #     properties = {
  #         "description": "Updating PMCID and DOI in main metadata"
  #         "no_of_articles": 1,
  #         "total_runtime": round(time.time() - start_time, 2),
  #     }
  # )


def downloadArticles(metadata: list, dir: str, error_file: str):
  """
    Downloads articles from PMC and stores them in local directory.
    Args:
        metadata: List of dictionaries containing metadata for each article.
    Returns:
      metadata: Updated metadata with license, FTP link, and downloaded filepath information.
    """
  # print("In downloadArticles()")
  start_time = time.monotonic()
  try:
    base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?"

    updated_articles = {}

    # Use ThreadPoolExecutor to run download_article for each article in parallel
    download_article_partial = partial(download_article, api_url=base_url, dir=dir, error_file=error_file)
    with concurrent.futures.ProcessPoolExecutor() as executor:
      futures = [executor.submit(download_article_partial, article) for article in metadata]
      for future in concurrent.futures.as_completed(futures):
        try:
          # print("Starting new download...")
          updated_article = future.result(timeout=15 * 60)  # Check result without blocking
          if updated_article:
            updated_articles[updated_article['pmid']] = updated_article
          # print("Updated article: ", updated_article)
        except Exception as e:
          print("Error downloading article:", e)
          with open(error_file, 'a') as f:
            f.write("Error in downloadArticles(): " + str(e) + "\n")

    # Update original metadata with updated articles
    for article in metadata:
      if article['pmid'] in updated_articles:
        article.update(updated_articles[article['pmid']])

    # print("Updated metadata after download: ", metadata)

    POSTHOG.capture(distinct_id="pubmed_extraction",
                    event="downloadArticles",
                    properties={
                        "description": "Download articles and update metadata",
                        "no_of_articles": 100,
                        "total_runtime": time.monotonic() - start_time,
                    })

    return metadata

  except Exception as e:
    #print("Error downloading articles: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in downloadArticles(): " + str(e) + "\n")
    return metadata


def download_article(article, api_url, dir, error_file):
  """
    Downloads the article from given FTP link and updates metadata with license, FTP link, and downloaded filepath information.
    This function is used within downloadArticles() function.
    Args:
        article: Dictionary containing metadata for the article.
        api_url: URL for the article download API.
        ftp: FTP connection object.
    Returns:
        article: Updated metadata for the article.
    """

  # print("Downloading articles...")
  try:
    if not article['live'] or article['pmcid'] is None:
      return

    # Proceed with download
    # Connect to FTP server anonymously
    ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov", timeout=15 * 60)
    ftp.login()

    if article['pmcid']:
      final_url = api_url + "id=" + article['pmcid']
      # print("\nDownload URL: ", final_url)

      xml_response = requests.get(final_url)
      extracted_data = extractArticleData(xml_response.text, error_file)
      # print("Extracted license and link data: ", extracted_data)

      if not extracted_data:
        article['live'] = False
        return

      article['license'] = extracted_data[0]['license']
      article['pubmed_ftp_link'] = extracted_data[0]['href'] if 'href' in extracted_data[0] else None

      ftp_url = urlparse(extracted_data[0]['href'])
      ftp_path = ftp_url.path[1:]
      # print("FTP path: ", ftp_path)

      filename = article['pmcid'] + "_" + ftp_path.split("/")[-1]
      local_file = os.path.join(dir, filename)

      try:
        with open(local_file, 'wb') as f:
          ftp.retrbinary('RETR ' + ftp_path, f.write)  # Download directly to file

        # print("Downloaded FTP file: ", local_file)
        article['filepath'] = local_file

        if filename.endswith(".tar.gz"):
          extracted_pdf_paths = extractPDF(local_file, dir, error_file, article['pmcid'])
          #print("Extracted PDFs from .tar.gz file: ", extracted_pdf_paths)
          article['filepath'] = ",".join(extracted_pdf_paths)
          os.remove(local_file)

      except concurrent.futures.TimeoutError:
        print("Download timeout reached.")

      ftp.quit()

      # print("\nUpdated metadata after download: ", article)
      return article
  except Exception as e:
    #print("Error in article download subprocess: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in download_article() PMID " + article['pmid'] + ": " + str(e) + "\n")
    return None


def extractPDF(tar_gz_filepath: str, dest_directory: str, error_file: str, pmcid: str):
  """
    Extracts PDF files from the downloaded .tar.gz file. The zipped folder contains other supplementary
    materials like images, etc. which are not extracted.
    Args:
        tar_gz_filepath: Path to the .tar.gz file.
    Returns:
        extracted_paths: List of paths to the extracted PDF files.
    """
  try:
    # print("Extracting PDF from: ", tar_gz_filepath)
    extracted_paths = []
    with tarfile.open(tar_gz_filepath, "r:gz") as tar:
      for member in tar:
        if member.isreg() and member.name.endswith(".pdf"):
          tar.extract(member, path=dest_directory)
          #print("Extracted: ", member.name)
          original_path = os.path.join(dest_directory, member.name)
          new_filename = pmcid + "_" + os.path.basename(member.name)
          new_path = os.path.join(dest_directory, new_filename)
          #print("New path: ", new_path)
          os.rename(original_path, new_path)
          extracted_paths.append(new_path)

    return extracted_paths
  except Exception as e:
    #print("Error extracting PDF: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in extractPDF() PMCID - " + pmcid + ": " + str(e) + "\n")
    return []


def extractArticleData(xml_string: str, error_file: str):
  """
    Extracts license information and article download link from the XML response.
    This function process XML response for single article.
    Args:
        xml_string: XML response from PMC download API.
    Returns:
        extracted_data: List of dictionaries containing license and download link for the article.
    """
  # print("In extractArticleData")

  try:
    root = ET.fromstring(xml_string)
    # if there is an errors (article not open-access), return empty list (skip article)
    if root.find(".//error") is not None:
      return []

    records = root.findall(".//record")
    extracted_data = []
    href = None

    for record in records:
      record_id = record.get("id")  # pmcid
      license = record.get("license")
      links = record.findall(".//link")

      for link in links:
        if link.get("format") == "pdf":
          href = link.get("href")
          break
      # if PDF link not found, use the available tgz link
      if not href:
        href = links[0].get("href")

      extracted_data.append({"record_id": record_id, "license": license, "href": href})

    return extracted_data
  except Exception as e:
    #print("Error extracting article data: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in extractArticleData(): " + str(e) + "\n")
      f.write("XML String: " + xml_string + "\n")
    return []


def upload_file(client, bucket_name, file_path, object_name, error_file, upload_log):
  """
    Uploads a single file to the Minio bucket.
    """
  try:
    client.fput_object(bucket_name, object_name, file_path)
    print(f"Uploaded: {object_name}")
    with open(upload_log, 'a') as f:
      f.write("uploaded: " + file_path + "\n")
    os.remove(file_path)
  except Exception as e:
    #print(f"Error uploading {object_name}: {e}")
    with open(error_file, 'a') as f:
      f.write("Error in upload_file(): " + str(e) + "\n")


def uploadToStorage(filepath: str, error_file: str):
  """
    Uploads all files present under given filepath to Minio bucket in parallel.
    """
  # print("in uploadToStorage()")
  try:
    bucket_name = "pubmed"

    found = MINIO_CLIENT.bucket_exists(bucket_name)
    if not found:
      MINIO_CLIENT.make_bucket(bucket_name)
      print("Created bucket", bucket_name)
    # else:
    #     print("Bucket", bucket_name, "already exists")
    #upload_log = error_file.split("_")[0] + ".txt"
    upload_log = "all_papers.txt"
    # Get all files to upload
    files = []
    for root, _, files_ in os.walk(filepath):
      for file in files_:
        file_path = os.path.join(root, file)
        object_name = file_path.split("/")[-1]
        files.append((MINIO_CLIENT, bucket_name, file_path, object_name, error_file, upload_log))

    # Use concurrent.futures ThreadPoolExecutor with limited pool size
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
      # Submit files in batches of 10
      for i in range(0, len(files), 10):
        batch_files = files[i:i + 10]
        futures = [executor.submit(upload_file, *args) for args in batch_files]
        done, not_done = concurrent.futures.wait(futures, timeout=180)

        for future in not_done:
          future.cancel()  # Cancel the future if it is not done within the timeout

        for future in done:
          try:
            future.result()  # This will raise any exceptions from upload_file
          except Exception as e:
            with open(error_file, 'a') as f:
              f.write("Error in upload_file(): " + str(e) + "\n")

        # for future in futures:
        #     future.result()  # This will raise any exceptions from upload_file

    return "success"
  except Exception as e:
    #print("Error uploading to storage: ", e)
    with open(error_file, 'a') as f:
      f.write("Error in uploadToStorage(): " + str(e) + "\n")
    return "failure"
