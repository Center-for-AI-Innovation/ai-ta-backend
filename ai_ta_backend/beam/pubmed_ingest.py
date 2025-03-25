"""
Endpoint for ingesting data from PubMed.
"""
from typing import Any, Callable, Dict, List
import beam

if beam.env.is_remote():
    import tarfile
    import concurrent.futures
    import time
    import boto3
    import os
    import sentry_sdk
    import requests
    import json
    import xml.etree.ElementTree as ET
    import ftplib
    from urllib.parse import urlparse
    from urllib.parse import quote


requirements = [
    "supabase==2.5.3",
    "boto3==1.28.79",
    "sentry-sdk==1.39.1",
]

image = beam.Image(
    python_version="python3.10",
    python_packages=requirements,
)

volume_path = "./pubmed_ingest"

ourSecrets = [
    "S3_BUCKET_NAME",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "BEAM_API_KEY",
]

def loader():
    print("Inside loader()")

    s3_client = boto3.client(
      's3',
      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )

    sentry_sdk.init(
      dsn=os.getenv("SENTRY_DSN"),
      # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
      traces_sample_rate=1.0,
      # Set profiles_sample_rate to 1.0 to profile 100% of sampled transactions.
      # We recommend adjusting this value in production.
      profiles_sample_rate=1.0,
      enable_tracing=True)
    
    return s3_client

@beam.endpoint(name="pubmed_ingest",
               workers=1,
               cpu=1,
               memory="3Gi",
               max_pending_tasks=10,
               timeout=60,
               keep_warm_seconds=60 * 3,
               secrets=ourSecrets,
               on_start=loader,
               image=image,
               volumes=[beam.Volume(name="pubmed_ingest", mount_path=volume_path)])
def pubmed_ingest(context, **inputs: Dict[str, Any]):
    """
    Main function
    Args:
        context: Context object
        inputs: Dict[str, Any]
    """
    s3_client = context.on_start_value
    course_name: str = inputs.get('course_name', '')
    search_query: str = inputs.get('search_query', '')
    journal_name: str = inputs.get('journal_name', '')
    journal_abbr: str = inputs.get('journal_abbr', '')
    article_title: str = inputs.get('article_title', '')
    from_date: str = inputs.get('from_date', '')
    to_date: str = inputs.get('to_date', '')
    pmc_id: str = inputs.get('pmc_id', '')

    # route to 2 functions based on the input - PMC ID or search query
    if pmc_id:
        # use Web Service OA API to fetch articles by PMC ID and date range
        return fech_articles_by_pmc_id(pmc_id, course_name, s3_client, from_date=from_date, to_date=to_date)
    
    if search_query or journal_name or article_title:
        # use EUtils API to fetch articles by journal, article title, text query, etc.
        return fetch_articles_by_query(search_query, journal_name, article_title, course_name, s3_client, from_date=from_date, to_date=to_date, journal_abbr=journal_abbr)


def fetch_articles_by_query(search_query, journal_name, article_title, course_name, s3_client, **kwargs):
    """
    This function downloads articles from PubMed using the EUtils API.
    Args:
        search_query: text query
        journal_name: journal name
        article_title: article title
        from_date: start date
        until_date: end date
    """

    from_date = kwargs.get('from_date', '')
    to_date = kwargs.get('to_date', '')
    journal_abbr = kwargs.get('journal_abbr', '')

    # create a directory to store the downloaded files
    #directory = '/tmp/pubmed_papers'
    directory = os.path.join(volume_path, "pubmed_papers")
    print("Directory: ", directory)
    os.makedirs(directory, exist_ok=True)


    # URL construction
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
    database = "db=pubmed"
    final_query = ""
    query_parts = []
    if journal_name:
        journal_query = journal_name.replace(" ", "+") + "[journal]"
        query_parts.append(journal_query)

    if journal_abbr:
        journal_abbr_query = journal_abbr(" ", "+") + "[ta]"
        query_parts.append(journal_abbr_query)
    
    if article_title:
        title_query = article_title + "[Title]"
        title_query = quote(title_query)
        query_parts.append(title_query)
    
    if search_query:
        search_query = search_query
        query_parts.append(search_query)

    # Join the queries with "+AND+" only if 2 or more are present
    if len(query_parts) > 1:
        final_query += "+AND+".join(query_parts)
    else:
        final_query += "".join(query_parts)
    
    if from_date:
        final_query += "&mindate=" + from_date
    
    if to_date:
        final_query += "&maxdate=" + to_date
    
    print("Final Query: ", final_query)
    
    final_url = base_url + database + "&term=" + final_query + "&retmode=json&retmax=100"
    print("Final URL: ", final_url)
    

    # Query the EUtils API
    response = requests.get(final_url)
    
    if response.status_code != 200:
        return "Error: " + str(response.status_code) + " - " + response.text
    
    data = response.json()
    total_records = int(data['esearchresult']['count'])
    print("Total Records: ", total_records)
    current_records = 0
    
    while current_records < total_records:
        # extract PubMed IDs and convert them to PMC IDs
        pubmed_id_list = data['esearchresult']['idlist']
        print("Number of records in current page: ", len(pubmed_id_list)) # should be retmax = 100
        id_str = ",".join(pubmed_id_list)
        current_pmc_ids = pubmed_id_converter(id_str)
        print("Number of PMC IDs returned: ", len(current_pmc_ids))

        for pmc_id, doi in current_pmc_ids:
            status = fech_articles_by_pmc_id(pmc_id, course_name, s3_client, doi=doi, journal_name=journal_name)
            print("Download status: ", status)
        
        # update current records count
        current_records += len(pubmed_id_list)
        print("Current number of records: ", current_records)

        # if next page exists, update next page url and call the API again
        retstart = current_records
        next_page_url = final_url + "&retstart=" + str(retstart)
        print("Next page URL: ", next_page_url)
        
        response = requests.get(next_page_url)
        if response.status_code != 200:
            return "Error in next page: " + str(response.status_code) + " - " + response.text
        
        data = response.json()
    
    return "Success"


def fech_articles_by_pmc_id(pmci_id, course_name, s3_client, **kwargs):
    """
    This function downloads articles from PubMed using the OA Web Service API.
    Search is based on PubMed ID, date range, and file format.
    Args:
        id: PubMed ID
        from_date: start date
        until_date: end date
    """
    doi = kwargs.get('doi', '')
    journal_name = kwargs.get('journal_name', '')
    from_date = kwargs.get('from_date', '')
    to_date = kwargs.get('to_date', '')

    # create a directory to store the downloaded files
    directory = os.path.join(volume_path, "pubmed_papers")
    print("Directory: ", directory)
    os.makedirs(directory, exist_ok=True)

    # URL construction
    main_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?"
    if pmci_id:
        main_url += "id=" + pmci_id
    if from_date:
        main_url += "&from=" + from_date
    if to_date:
        main_url += "&until=" + to_date
    print("URL: ", main_url)

    # Query the OA Web Service API
    xml_response = requests.get(main_url)
    root = ET.fromstring(xml_response.text)
    resumption = root.find(".//resumption")

    # Process the first page
    downloaded_files = process_page(xml_response, directory)

    # Loop through subsequent pages if resumption tag is found
    while resumption is not None:
        # Get the next page link
        resumption_url = resumption.find(".//link").get("href")
        print("Next page: ", resumption_url)

        # Query for the next set of articles
        xml_response = requests.get(resumption_url)
        root = ET.fromstring(xml_response.text)
        resumption = root.find(".//resumption")

        # Process the current page
        downloaded_files = process_page(xml_response, directory)

    # Ingest into UIUC.Chat
    for file in downloaded_files:

        # upload to s3
        print("Uploading: ", file)
        filename = file.split("/")[-1]
        local_path = file
        s3_path = "courses/" + course_name + "/" + filename   
        print("S3 Path: ", s3_path)
        s3_client.upload_file(local_path, os.environ['S3_BUCKET_NAME'], s3_path)

        # send for ingest
        if not doi:
            doi_url = ""
        else:
            doi_url = "https://doi.org/" + doi
        
        payload = {
            "course_name": course_name,
            "readable_filename": filename,
            "s3_paths": [s3_path],
            "base_url": "",
            "url": doi_url,
        }

        if journal_name:
            payload["groups"] = [journal_name]
        print("Ingest Payload: ", payload)

        beam_url = 'https://app.beam.cloud/taskqueue/ingest_task_queue/latest'
        print("Beam API key: ", os.getenv('BEAM_API_KEY'))
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Authorization': f"Bearer {os.environ['BEAM_API_KEY']}",
            'Content-Type': 'application/json',
        }

        response = requests.post(beam_url, headers=headers, json=payload)
        print("Response: ", response.text)
    
    return "Success"


def process_page(xml_response, directory):
    
    records = extract_record_data(xml_response.text)
    print("Total records: ", len(records))

    if len(records) > 0:
        # download articles
        download_status = downloadFromFTP(records, directory, ftp_address="ftp.ncbi.nlm.nih.gov")
    else:
        download_status = []

    print("Downloaded files: ", download_status)
    return download_status


def extract_record_data(xml_string):
    """
    It is used to parse the response from the OA Web Service API - process_page().
    Extracts record ID, license, and href elements from an XML string.
    Args:
        xml_string: XML string --> Response from the OA Web Service API
    Returns:
        extracted_data: list of dictionaries
    """
    root = ET.fromstring(xml_string)
    records = root.findall(".//record")
    extracted_data = []
    href = None

    for record in records:
        record_id = record.get("id")
        license = record.get("license")
        links = record.findall(".//link")
        for link in links:
            # check for PDF links first
            if link.get("format") == "pdf":
                href = link.get("href")
                break
        # if PDF link not found, use the available tgz link
        if not href:
            href = links[0].get("href")

        extracted_data.append({
            "record_id": record_id,
            "license": license,
            "href": href
        })
    print("Extracted data: ", extracted_data)
    return extracted_data


def downloadFromFTP(paths, local_dir, ftp_address):
    """
    This function downloads files from the FTP server. 
    Args:
        paths: list of FTP paths
        local_dir: local directory to save the files
        ftp_address: ftp address
    """
    print("Inside downloadFromFTP()")

    # Connect to the FTP server
    ftp = ftplib.FTP(ftp_address)
    ftp.login() 

    # Download each file in the list
    downloaded_files = []
    for path in paths:
        ftp_url = urlparse(path['href'])
        ftp_path = ftp_url.path[1:]
        #print("Downloading from FTP path: ", ftp_path)
        record_id = path['record_id']
        filename = record_id + "_" + ftp_path.split('/')[-1]
        local_file = os.path.join(local_dir, filename)
        print("Local file: ", local_file)

        with open(local_file, 'wb') as f:
            ftp.retrbinary("RETR " + ftp_path, f.write)
        print("Downloaded PDF: ", filename)

        # pdf or tar.gz?
        if filename.endswith(".pdf"):
            print("Downloaded PDF: ", filename)
            downloaded_files.append(local_file)
        else:
            extracted_pdf_paths = extract_pdf(local_file, local_dir, record_id)
            print("Extracted PDFs: ", extracted_pdf_paths)
            downloaded_files.extend(extracted_pdf_paths)
        
    ftp.quit()
    return downloaded_files

def extract_pdf(tar_gz_file, dest_directory="pubmed_papers", record_id=""):
    """
    Extracts a PDF file from a tar.gz archive and stores it in the same folder
    Args:
        tar_gz_file: The path to the tar.gz file.
    """

    try:
        extracted_paths = []
        with tarfile.open(tar_gz_file, "r:gz") as tar:
            for member in tar:
                if member.isreg() and member.name.endswith(".pdf"):
                    tar.extract(member, path=dest_directory)
                    original_path = os.path.join(dest_directory, member.name)
                    new_filename = record_id + "_" + os.path.basename(member.name)
                    new_path = os.path.join(dest_directory, new_filename)
                    print("New path: ", new_path)
                    os.rename(original_path, new_path)
                    extracted_paths.append(new_path)
        # delete tar.gz file
        os.remove(tar_gz_file)

        return extracted_paths
    except Exception as e:
        print("Error in extracting PDF: ", e)
        return []


def pubmed_id_converter(id: str):
    """
    This function is used to convert DOI to PubMed ID.
    Can also be used to convert PubMed ID to DOI.
    """
    pmcid_list = []
    base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    app_details = "?tool=ncsa_uiuc&email=caiincsa@gmail.com"
    url = base_url + app_details + "&ids=" + id
    
    response = requests.get(url)
    #print("Response: ", response.text)
    root = ET.fromstring(response.text)
    records = root.findall(".//record")
    for record in records:
        pmcid = record.get("pmcid")
        doi = record.get("doi")
        if pmcid:
            pmcid_list.append((pmcid, doi))
    
    return pmcid_list
