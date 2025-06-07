import os
import shutil
import requests
import json
import arxiv
import crossref_commons.retrieval
import xml.etree.ElementTree as ET
import ftplib
from urllib.parse import urlparse
import urllib.parse

# from ai_ta_backend.aws import upload_data_files_to_s3
# from ai_ta_backend.vector_database import Ingest

import supabase
import tarfile
import concurrent.futures
import time

# Below functions hit API endpoints from sites like arXiv, Elsevier, and Sringer Nature to retrieve journal articles
SPRINGER_API_KEY = os.environ.get('SPRINGER_API_KEY')
ELSEVIER_API_KEY = os.environ.get('ELSEVIER_API_KEY')
ELSEVIER_TEST_API_KEY = os.environ.get('ELSEVIER_TEST_API_KEY')

SUPABASE_CLIENT = supabase.create_client(  # type: ignore
      supabase_url=os.getenv('SUPABASE_URL'),  # type: ignore
      supabase_key=os.getenv('SUPABASE_API_KEY'))  # type: ignore

##------------------------ DOI FUNCTIONS ------------------------##

def getFromDoi(doi: str, course_name: str):
    """
    This function takes DOI string as input and downloads the article from the publisher's website.
    Publishers covered: Springer Nature, Elsevier, PubMed
    Args:
        doi: DOI string
    """
    # get metadata from crossref
    metadata = get_article_metadata_from_crossref(doi)
    print("Publisher: ", metadata['publisher'])
    print("Content domain: ", metadata['content-domain'])
    publisher = metadata['publisher'].lower().split()
    #print("Metadata:", metadata)

    if 'springer' in publisher:
        # download from springer
        downloadSpringerFulltext(doi=doi, course_name=course_name)
    elif 'elsevier' in publisher:
        # download from elsevier
        downloadElsevierFulltextFromId(id=doi, id_type='doi', course_name=course_name)
    else:
        print("No direct openaccess link found. Searching PubMed...")

        pmcid = pubmed_id_converter(doi)
        if pmcid:
            print("Article found in PubMed. Downloading...")
            downloadPubmedArticles(id=pmcid, course_name=course_name)
        else:
            print("Article not found in our current databases, please try again later.")

    return "success"

def get_article_link_from_doi(doi: str) -> str:
    """
    This function calls the doi.org API to retrieve the link to the journal article.
    """    
    prefix = "https://doi.org/api/handles/"

    url = prefix + doi
    response = requests.get(url)
    data = response.json()
    article_link = data['values'][0]['data']['value']

    return article_link

def get_article_metadata_from_crossref(doi: str):
    """
    This function calls the crossref.org API to retrieve the metadata of a journal article.
    """    
    metadata = crossref_commons.retrieval.get_publication_as_json(doi)
    return metadata

##------------------------ ARXIV API FUNCTIONS ------------------------##

def get_arxiv_fulltext(query = "", ids = None, course_name = None):
    """
    This function retrieves journal articles from arXiv
    based on search query or article IDs or a combination of both.
    """
    
    if ids:
        search = arxiv.Search(id_list=[ids], 
                          max_results=10, 
                          sort_by = arxiv.SortCriterion.SubmittedDate)
    elif query:
        search = arxiv.Search(query=query, 
                            max_results=10, 
                            sort_by = arxiv.SortCriterion.SubmittedDate)

    directory = os.path.join(os.getcwd(), 'arxiv_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    for result in arxiv.Client().results(search):
        print("Downloading paper: ", result.title)
        result.download_pdf(dirpath=directory)

    return "success"


##------------------------ SPRINGER NATURE API FUNCTIONS ------------------------##

def downloadSpringerFulltext(issn=None, subject=None, journal=None, title=None, doi=None, course_name=None):
    """
    This function uses the Springer Nature API to download openaccess journal articles.
    Args:
        issn: limit to ISSN number of the journal/book
        subject: limit articles to a specific subject - Chemistry, Physics, etc.
        journal: limit to keywords occuring in journal title
        title: limit to keywords occuring in article title
    The initial API response returns a list of articles with metadata.
    
    """
    # create directory to store files
    directory = os.path.join(os.getcwd(), 'springer_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    # set headers
    api_url = "http://api.springernature.com/openaccess/json?q="
    headers = {'Accept': 'application/json'}

    # form the query URL based on the input parameters received
    if doi:
        query_str = "doi:" + doi
    elif issn:
        query_str = "issn:" + issn
    elif journal:   
        journal = "%22" + journal.replace(" ", "%20") + "%22"
        query_str = "journal:" + journal
    elif title:
        title = "%22" + title.replace(" ", "%20") + "%22"
        query_str = "title:" + title
    elif subject:
        query_str = "subject:" + subject
    else:
        return "No query parameters provided"
    
    main_url = api_url + query_str + "&api_key=" + str(SPRINGER_API_KEY)
    print("Full URL: ", main_url)
    
    
    response = requests.get(main_url, headers=headers)
    print("Status: ", response.status_code)

    if response.status_code != 200:
        return "Error: " + str(response.status_code) + " - " + response.text

    data = response.json()
    # check for total number of records 
    total_records = int(data['result'][0]['total'])
    print("Total records: ", total_records)
    current_records = 0

    while current_records < total_records:
        # check if nextPage exists
        if 'nextPage' in data:
            next_page_url = "http://api.springernature.com" + data['nextPage']
        else:
            next_page_url = None

        # multi-process all records in current page
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(downloadPDFSpringer, record, directory) for record in data['records']]
            for f in concurrent.futures.as_completed(results):
                print(f.result())

        # update current records count
        current_records += int(len(data['records']))

        # if next page exists, update next page url and call the API again
        if next_page_url:
            # API key is already present in the URL
            response = requests.get(next_page_url, headers=headers)
            if response.status_code != 200:
                return "Error in next page: " + str(response.status_code) + " - " + response.text
            
            data = response.json()
                
    # send the docs for ingest via beam

    # update document groups for the files  

    # # Delete files from local directory
    # shutil.rmtree(directory)
                                
    return "success"

def downloadPDFSpringer(record: dict, directory: str):
    """
    This function takes a record from the Springer API response and downloads the PDF file.
    It is called in a multi-process loop in downloadSpringerFulltext().
    Args:
        record: dictionary containing DOI and other metadata
        directory: local directory to save the files
    """
    headers = {'Accept': 'application/json'}

    if len(record['url']) < 1:
        return "No download link found for DOI: " + record['doi']

    # extract URL
    url = record['url'][0]['value'] + "?api_key=" + str(SPRINGER_API_KEY)
    url_response = requests.get(url, headers=headers)
    if url_response.status_code != 200:
        return "Error in accessing article link: " + str(url_response.status_code) + " - " + url_response.text
    url_data = url_response.json()

    # extract PDF link
    pdf_link = None
    links = url_data['link']
    for link in links:
        if link['content-type'] == 'application/pdf' and link['intended-application'] == 'text-mining':
            pdf_link = link['URL']
            #print("PDF Link: ", pdf_link)
            break
    if not pdf_link:
        return "No PDF link found for DOI: " + record['doi']
    
    # download PDF
    filename = record['doi'].replace("/", "_")
    try:
        response = requests.get(pdf_link)
        if response.status_code != 200:
            return "Error in downloading PDF: " + str(response.status_code) + " - " + response.text
        
        with open(directory + "/" + filename + ".pdf", "wb") as f:  # Open a file in binary write mode ("wb")
            for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
                f.write(chunk)
        print("Downloaded: ", filename)
        return "success"
    except Exception as e:
        return "Error in downloading PDF: " + str(e)

    
    

##------------------------ ELSEVIER API FUNCTIONS ------------------------##

def downloadElsevierFulltextFromId(id: str, id_type: str, course_name: str):
    """
    This function downloads articles from Elsevier for a given DOI.
    Modify the function to accept all sorts of IDs - pii, pubmed_id, eid
    Args:
        id: DOI, PII, EID, or Pubmed ID
        id_type: type of ID - doi, pii, eid, pubmed_id
        course_name: course name
    """

    # create directory to store files
    directory = os.path.join(os.getcwd(), 'elsevier_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    headers = {'X-ELS-APIKey': ELSEVIER_API_KEY, 'Accept':'application/pdf'}
    url = 'https://api.elsevier.com/content/article/'

    if id_type == "doi":
        url += "doi/" + id
    elif id_type == "eid":
        url += "eid/" + id
    elif id_type == "pii":
        url += "pii/" + id
    elif id_type == "pubmed_id":
        url += "pubmed_id/" + id
    else:
        return "No query parameters provided"

    response = requests.get(url, headers=headers)
    #print("Response content type: ", response.headers)
    if response.status_code != 200:
        return "Error in download function: " + str(response.status_code) + " - " + response.text
    
    filename = id.replace("/", "_") + ".pdf"
    with open(directory + "/" + filename, "wb") as f:  # Open a file in binary write mode ("wb")
        for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
            f.write(chunk)
    

    # # upload to s3
    # s3_paths = upload_data_files_to_s3(course_name, directory)

    # # Delete files from local directory
    # shutil.rmtree(directory)

    # # ingest into QDRANT
    # ingest = Ingest()
    # journal_ingest = ingest.bulk_ingest(s3_paths, course_name=course_name)

    return "success"

def searchScopusArticles(course: str, search_str: str, title: str, pub: str, subject: str, issn: str):
    """
    This function is used for a text-based search in Scopus (Elsevier service).
    1. Use Scopus API to search for articles based on ISSN, publication title, article title, or subject area.
    2. Extract PII from the results and call downloadElsevierFulltextFromDoi() to download the full-text to a local directory.
    3. Upload the files to a supabase bucket.

    Args:
        course: course name
        query: search query
        title: article title
        journal: journal title
        subject: subject area
        issn: ISSN number ---> if targeting a journal, its better to search by ISSN.
    """
    # log start time
    start_time = time.monotonic()

    # create directory to store files locally
    directory = os.path.join(os.getcwd(), 'elsevier_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    # set headers
    headers = {'X-ELS-APIKey': ELSEVIER_API_KEY, 'Accept':'application/json'}
    count = 10 # rate limit of 10 requests per second

    # form the query URL based on the input parameters received
    base_url = "https://api.elsevier.com/content/search/scopus?"
    query = "query="
    if issn:
        query += "ISSN(" + issn + ")"
    if pub:
        query += "SRCTITLE(" + pub + ")"
    if title:
        query += "TITLE(" + title + ")"
    if subject:
        query += "SUBJAREA(" + subject + ")"
    if search_str:
        query += search_str

    final_url = base_url + query + "OPENACCESS(1)" + "&count=" + str(count)
    print("Final original URL: ", final_url)

    encoded_url = urllib.parse.quote(final_url, safe=':/?&=')
    print("Encoded URL: ", encoded_url)

    response = requests.get(encoded_url, headers=headers)
    if response.status_code != 200:
        return "Error: " + str(response.status_code) + " - " + response.text
    
    search_response = response.json()
    total_records = int(search_response['search-results']['opensearch:totalResults'])
    print("Total records: ", total_records)
    current_records = 0
        
    # iterate through results and extract pii
    while current_records < total_records:
        # extract next page link if present
        links = search_response['search-results']['link']
        next_page_url = None
        for link in links:
            if link['@ref'] == 'next':
                next_page_url = link['@href']
                break

        # multi-process all records in this page - extract PII and call download function on it
        records = search_response['search-results']['entry']
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(downloadElsevierFulltextFromId, record['pii'], 'pii', course) for record in records]
            for f in concurrent.futures.as_completed(results):
                try:
                    print(f.result())
                except Exception as e:
                    print(f"Error occurred during download: {e}")

        # update current records count
        current_records += len(records)
        print("Current records: ", current_records)

        # if next page exists, update next page url and call the API again
        if next_page_url:            
            response = requests.get(next_page_url, headers=headers)
            if response.status_code != 200:
                return "Error in next page: " + str(response.status_code) + " - " + response.text
            else:
                search_response = response.json()

             
    # after all records are downloaded, upload to supabase bucket     
    supabase_bucket_path = "publications/elsevier_journals/mbio"      
    try:
        for root, directories, files in os.walk(directory):
            for file in files:
                filepath = os.path.join(root, file)
                print("Uploading: ", file)
                upload_path = "elsevier_papers/" + file
                try:
                    with open(filepath, "rb") as f:
                        res = SUPABASE_CLIENT.storage.from_(supabase_bucket_path).upload(file=f, path=file, file_options={"content-type": "application/pdf"})
                except Exception as e:
                    print("Error: ", e)

        # remove local files
        shutil.rmtree(directory)  
    except Exception as e:
        print("Error: ", e)
    
    # log end time
    print(f"⏰ Runtime: {(time.monotonic() - start_time):.2f} seconds")

    return "success"


def searchScienceDirectArticles(course_name: str, search_str: str, article_title: str, publication_title: str):
    """
    This function is used for a text-based search in ScienceDirect (Elsevier service).
    1. Use ScienceDirect API to search for articles based on input parameters. Need to make a PUT request.
    2. Extract PII/DOI from the results and call downloadElsevierFulltextFromDoi() to download the full-text to a local directory.
    3. Upload the files to a supabase bucket.
    Args:
        search_str: free text search query - will search against all text fields in articles
        article_title: title of the article or book chapter
        publication_title: title of journal or book
    """
    # log start time
    start_time = time.monotonic()

    # create directory to store files locally
    directory = os.path.join(os.getcwd(), 'elsevier_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # create payload for API request
    data = {
        "filter": {
            "openAccess": True
        },
        "display": {
            "offset": 0,
            "show": 10
        }
    }

    # read parameters from request - use quotation marks for exact match, need at least one of title, qs or author.
    if search_str:
        data["qs"] = search_str
    if article_title:
        data["title"] = "\"" + article_title + "\""
    if publication_title:
        data["pub"] = "\"" + publication_title + "\""
        if not search_str:
            data["qs"] = "\"" + publication_title + "\""    

    #data = json.dumps(data)
    #data = json.loads(data)
    print("Data: ", data)

    url = "https://api.elsevier.com/content/search/sciencedirect"
    
    #headers = {'X-ELS-APIKey': ELSEVIER_API_KEY, 'Accept':'application/json'}
    headers = {'X-ELS-APIKey': ELSEVIER_TEST_API_KEY, 'Accept':'application/json'}
    response = requests.put(url, headers=headers, json=data)
    
    if response.status_code != 200:
        return "Error: " + str(response.status_code) + " - " + response.text
    
    response_data = response.json()
    total_records = response_data['resultsFound']
    print("Total records: ", total_records)
    current_records = 810

    while current_records < total_records:
        # iterate through results and extract pii
        if 'results' not in response_data:
            print("response_data: ", response.text)
            print("headers: ", response.headers)
            break
        records = response_data['results']
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(downloadElsevierFulltextFromId, record['pii'], 'pii', course_name) for record in records]
        
        current_records += len(records)
        print("Current records: ", current_records)

        # update the offset parameter in data and call the API again
        data["display"]["offset"] += current_records
        response = requests.put(url, headers=headers, json=data)

        if response.status_code != 200:
            return "Error: " + str(response.status_code) + " - " + response.text
        
        response_data = response.json()
        
            
    
    print(f"⏰ Total Download Time: {(time.monotonic() - start_time):.2f} seconds")
    
    # after all records are downloaded, upload to supabase bucket
    # supabase_bucket_path = "publications/elsevier_journals/trends_in_microbiology"
    # try:
    #     for root, directories, files in os.walk(directory):
    #         for file in files:
    #             filepath = os.path.join(root, file)
    #             print("Uploading: ", file)
    #             upload_path = "elsevier_papers/" + file
    #             try:
    #                 with open(filepath, "rb") as f:
    #                     res = SUPABASE_CLIENT.storage.from_(supabase_bucket_path).upload(file=f, path=upload_path, file_options={"content-type": "application/pdf"})
    #             except Exception as e:
    #                 print("Error: ", e)

    #     # remove local files
    #     shutil.rmtree(directory)
    # except Exception as e:
    #     print("Error: ", e)
    
    # log end time
    print(f"⏰ Total Runtime: {(time.monotonic() - start_time):.2f} seconds")
        
    return "success"


##------------------------ PUBMED API FUNCTIONS ------------------------##

def downloadPubmedArticles(id, course_name, **kwargs):
    """
    This function downloads articles from PubMed using the OA Web Service API.
    Search is based on PubMed ID, date range, and file format.
    Args:
        id: PubMed ID
        from_date: start date
        until_date: end date
        format: file format - pdf or tgz
    """
    from_date = kwargs.get('from_date', None)
    until_date = kwargs.get('until_date', None)
    format = kwargs.get('format', None)

    directory = os.path.join(os.getcwd(), 'pubmed_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    main_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?"
    if id:
        main_url += "id=" + id
    elif from_date and until_date:
        main_url += "from=" + from_date + "&until=" + until_date
    elif from_date:
        main_url += "from=" + from_date
    else:
        return "No query parameters provided"
    
    if format != None and format in ['tgz', 'pdf']:
        main_url += "&format=" + format

    #print("Full URL: ", main_url)

    xml_response = requests.get(main_url)
    root = ET.fromstring(xml_response.text)
    resumption = root.find(".//resumption")

    while resumption is not None: # download current articles and query 
        # parse xml response and extract pdf links and other metadata
        records = extract_record_data(xml_response.text)
        # add a check here for license and download only CC articles

        #print("Total records: ", len(records))
        if len(records) > 0:
            # download articles
            download_status = downloadFromFTP(records, directory, ftp_address="ftp.ncbi.nlm.nih.gov")

        # query for next set of articles    
        resumption_url = resumption.find(".//link").get("href")
        #print("Resumption URL: ", resumption_url)

        xml_response = requests.get(resumption_url)
        root = ET.fromstring(xml_response.text)
        resumption = root.find(".//resumption")

    # download current articles if resumption is None
    records = extract_record_data(xml_response.text)
    # add a check here for license and download only CC articles

    #print("Current total records: ", len(records))
    if len(records) > 0:
        # download articles
        download_status = downloadFromFTP(records, directory, ftp_address="ftp.ncbi.nlm.nih.gov")

    # upload to supabase bucket
    # try:
    #     for root, directories, files in os.walk(directory):
    #         for file in files:
    #             filepath = os.path.join(root, file)
    #             print("Uploading: ", file)
    #             uppload_path = "pubmed_articles/" + file
    #             with open(filepath, "rb") as f:
    #                 res = SUPABASE_CLIENT.storage.from_("publications").upload(file=f, path=uppload_path, file_options={"content-type": "application/pdf"})
    #                 print("Upload response: ", res)
            
    # except Exception as e:
    #     print("Error: ", e)
    # # upload to s3
    # s3_paths = upload_data_files_to_s3(course_name, directory)

    # # Delete files from local directory
    #shutil.rmtree(directory)

    # # ingest into QDRANT
    # ingest = Ingest()
    # journal_ingest = ingest.bulk_ingest(s3_paths, course_name=course_name)

    return "success"

def searchPubmedArticlesWithEutils(course: str, search: str, title: str, journal: str):
    """
    This function is used for a text-based search in PubMed using the E-Utilities API.
    Args:
        course: course name
        search: search query
        title: article title
        journal: journal title
    """
    start_time = time.monotonic()

    directory = os.path.join(os.getcwd(), 'pubmed_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
    database = "db=pubmed"
    final_query = "term="
    
    title_query = journal_query = search_query = ""
    if title:
        title_query = title.replace(" ", "+") + "[Title]"
        final_query += title_query + "+AND+"
    else:
        final_query += title_query
    if journal:
        journal_query = journal.replace(" ", "+") + "[ta]"
        final_query += journal_query + "+AND+"
    else:
        final_query += journal_query
    if search:
        search_query = search.replace(" ", "+")
        final_query += search_query
    
    final_url = base_url + database + "&" + final_query + "&retmode=json&retmax=100"
    print("Final URL: ", final_url)
    response = requests.get(final_url)

    if response.status_code != 200:
        return "Error: " + str(response.status_code) + " - " + response.text

    data = response.json()
    
    total_records = int(data['esearchresult']['count'])
    current_records = 0
    current_records = 0

    print("Total Records: ", total_records)
    pmid_list = []
    

    while current_records < total_records:
        # extract ID and convert them to PMC ID
        id_list = data['esearchresult']['idlist']
        print("Number of records in current page: ", len(id_list))
        id_str = ",".join(id_list)
        current_pmc_ids = pubmed_id_converter(id_str)
        print("Number of PMC IDs: ", len(current_pmc_ids))

        # extract the PMIDs which do not have a PMCID
        diff_ids = list(set(id_list) - set(current_pmc_ids))
        print("Current PM IDs not present as PMC Ids: ", len(diff_ids))
        print("Diff IDs: ", diff_ids)
        pmid_list += diff_ids
        

        # call pubmed download here - parallel processing
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [executor.submit(downloadPubmedArticles, id, course) for id in current_pmc_ids]
            
        # update current records count
        current_records += len(id_list)

        # if next page exists, update next page url and call the API again
        retstart = current_records
        next_page_url = base_url + database + "&" + final_query + "&retmode=json&retmax=100&retstart=" + str(retstart)
        print("Next page URL: ", next_page_url)
        response = requests.get(next_page_url)
        if response.status_code != 200:
            return "Error in next page: " + str(response.status_code) + " - " + response.text
        data = response.json()
    
    # check if IDs from pmid_list are present in elsevier
    print("PMIDs without PMC IDs: ", len(pmid_list))
    if len(pmid_list) > 0:
        print("Downloading from Elsevier...")
        with concurrent.futures.ProcessPoolExecutor() as executor:
            batch_size = 10
            batches = [pmid_list[i:i+batch_size] for i in range(0, len(pmid_list), batch_size)]
            results = []
            for batch in batches:
                batch_results = [executor.submit(downloadElsevierFulltextFromId, pmid, 'pubmed_id', course) for pmid in batch]
                results.extend(batch_results)
    
    print(f"⏰ Total Download Runtime: {(time.monotonic() - start_time):.2f} seconds")
    
    # upload to supabase bucket
    count = 0
    try:
        for root, directories, files in os.walk(directory):
            for file in files:
                filepath = os.path.join(root, file)
                upload_path = file
                try:
                    with open(filepath, "rb") as f:
                        res = SUPABASE_CLIENT.storage.from_("publications/pubmed_journals/virus_evolution").upload(file=f, path=upload_path, file_options={"content-type": "application/pdf"})
                    count += 1
                except Exception as e:
                    print("Error: ", e)
                print("Uploaded: ", count)    
            
    except Exception as e:
        print("Error: ", e)
    
    # log end time
    print(f"⏰ Total Runtime with Supabase upload: {(time.monotonic() - start_time):.2f} seconds")
        
    return "success"


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
        if pmcid:
            pmcid_list.append(pmcid)
    
    return pmcid_list


def extract_record_data(xml_string):
    """
    It is used to parse the response from the OA Web Service API - downloadPubmedArticles().
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
    This function downloads files from an FTP server. 
    Used in conjunction with downloadPubmedArticles().
    Args:
        paths: list of FTP paths
        local_dir: local directory to save the files
        ftp_address: ftp address
    """
    # Connect to the FTP server anonymously
    ftp = ftplib.FTP(ftp_address)
    ftp.login()  # Use anonymous login

    # Download each file in the list
    for path in paths:
        ftp_url = urlparse(path['href'])
        ftp_path = ftp_url.path[1:]
        #print("Downloading from FTP path: ", ftp_path)

        filename = ftp_path.split('/')[-1]
        local_file = os.path.join(local_dir, filename)
        with open(local_file, 'wb') as f:
            ftp.retrbinary("RETR " + ftp_path, f.write)
        print("Downloaded: ", filename)
    
    # for path in paths:
    #     ftp_url = urlparse(path['href'])
    #     ftp_path = ftp_url.path[1:]
    #     #print("Downloading from FTP path: ", ftp_path)

    #     filename = ftp_path.split('/')[-1]
    #     local_file = os.path.join(local_dir, filename)
    #     with open(local_file, 'wb') as f:
    #         ftp.retrbinary("RETR " + ftp_path, f.write)
    #     #print("Downloaded: ", filename)

    #     # if filename ends in tar.gz, extract the pdf and delete the tar.gz
    #     if filename.endswith(".tar.gz"):
    #         extracted_pdf = extract_pdf(local_file)
    #         #print("Extracted PDF: ", extracted_pdf)

    #         filename = os.path.basename(filename)
    #         new_pdf_name = filename.replace('.tar.gz', '.pdf')
            
    #         new_pdf_path = os.path.join(local_dir, new_pdf_name)
    #         old_pdf_path = os.path.join(local_dir, extracted_pdf)
    #         os.rename(old_pdf_path, new_pdf_path)

    #         # delete the tar.gz file
    #         os.remove(local_file)
    #         os.remove(old_pdf_path)

    ftp.quit()
    return "success"


def extract_pdf(tar_gz_file):
  """
  Extracts a PDF file from a tar.gz archive and stores it in the same folder.

  Args:
    tar_gz_file: The path to the tar.gz file.
  """

  with tarfile.open(tar_gz_file, "r:gz") as tar:
    for member in tar:
      # Check if it's a regular file and ends with .pdf extension
      if member.isreg() and member.name.endswith(".pdf"):
        # get the file name
        
        # Extract the file to the same directory
        tar.extract(member, path="pubmed_papers")

        return member.name