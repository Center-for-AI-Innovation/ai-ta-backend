import os
import json
import time
import pandas as pd
import shutil
import requests
import supabase
import concurrent.futures
from crossref.restful import Works, Journals
from ai_ta_backend.database import aws, sql
import backoff

SPRINGER_API_KEY = os.environ.get('SPRINGER_API_KEY')
CC_LICENSES = {
    "http://creativecommons.org/licenses/by/4.0/": "CC BY",
    "http://creativecommons.org/licenses/by-nc/4.0/": "CC BY-NC",
    "http://creativecommons.org/licenses/by-nc-nd/4.0/": "CC BY-NC-ND",
    "http://creativecommons.org/licenses/by-nc-sa/4.0/": "CC BY-NC-SA"
}

OTHER_LICENSES = {
    "http://onlinelibrary.wiley.com/termsAndConditions#vor": "wiley_tnc",
    "http://onlinelibrary.wiley.com/termsAndConditions#am": "wiley_tnc",
    "http://doi.wiley.com/10.1002/tdm_license_1.1": "wiley_tdm"
}

s3_client = aws.AWSStorage()
aws_bucket = os.getenv('S3_BUCKET_NAME')
supabase_client = supabase.create_client(  # type: ignore
      supabase_url=os.environ['SUPABASE_URL'], supabase_key=os.environ['SUPABASE_API_KEY'])


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
    print("in downloadSpringerFulltext")
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
        try:
            if 'nextPage' in data:
                next_page_url = "http://api.springernature.com" + data['nextPage']
            else:
                next_page_url = None

            # multi-process all records in current page
            # with concurrent.futures.ProcessPoolExecutor() as executor:
            #     results = []
            #     for i in range(0, len(data['records']), 3):
            #         batch = data['records'][i:i+3]
            #         batch_results = [executor.submit(downloadPDFSpringer, record, directory) for record in batch]
            #         results.extend(batch_results)
            #     for f in concurrent.futures.as_completed(results):
            #         print(f.result())
            print("Total records: ", len(data['records']))
            
            for i in range(len(data['records'])):
                print("i: ", i)
                print("Processing record: ", data['records'][i])
                print("\n")
                article_metadata = downloadPDFSpringer(data['records'][i], directory)
                article_metadata['issn'] = issn

                # write article metadata to CSV file
                metadata_csv = "springer_metadata.csv"
                metadata_df = pd.DataFrame([article_metadata])
                if not os.path.exists(metadata_csv):
                    metadata_df.to_csv(metadata_csv, index=False)
                else:
                    metadata_df.to_csv(metadata_csv, mode='a', header=False, index=False)
                
            # update current records count
            current_records += int(len(data['records']))

            # if next page exists, update next page url and call the API again
            if next_page_url:
                # API key is already present in the URL
                response = requests.get(next_page_url, headers=headers)
                if response.status_code != 200:
                    return "Error in next page: " + str(response.status_code) + " - " + response.text
                
                data = response.json()
        except Exception as e:
            print(e)

    # print("Course name: ", course_name)
    # prep payload for beam ingest
    # ingest_data = []
    
    # upload files to S3 bucket
    # for file in os.listdir(directory):
    #     doi = file[:-4]
    #     doi = doi.replace("_", "/")
    #     doi_link = f"https://doi.org/{doi}"
    #     data = {
    #         "course_name": course_name,
    #         "groups": "springer_open",
    #         "s3_paths": "courses/" + course_name + "/" + file, # type: ignore
    #         "readable_filename": file,
    #         "base_url": "",
    #         "url": doi_link,
    #         "journal": "",
    #     }
    #     s3_path = "courses/" + course_name + "/" + file # type: ignore
    #     s3_client.upload_file(directory + "/" + file, aws_bucket, s3_path)  # type: ignore
    #     ingest_data.append(data)
    
    # # save ingest data to csv
    # ingest_df = pd.DataFrame(ingest_data)
    # csv_file = "publications_data.csv"
    # if not os.path.exists(csv_file):
    #     ingest_df.to_csv(csv_file, index=False)
    # else:
    #     ingest_df.to_csv(csv_file, mode='a', header=False, index=False)


    # # call ingest
    # beam_url = "https://3xn8l.apps.beam.cloud"
    # headers = {
    # "Content-Type": "application/json",
    # "Authorization": "Basic " + os.getenv('BEAM_AUTH_TOKEN')    # type: ignore
    # }

    # pubs_data = pd.read_csv(csv_file)

    # for row in pubs_data.iterrows():
    #     payload = {
    #     "course_name": "cropwizard-pro",
    #     "s3_paths": [row[1]["s3_paths"]],
    #     "readable_filename": row[1]["readable_filename"],
    #     "base_url": "",
    #     "url": row[1]["url"],
    #     "groups": ["Springer", "CC-BY", "Research Paper"]
    #     }
    #     print(payload)
    #     payload = json.dumps(data)
    #     response = requests.post(beam_url, headers=headers, data=payload)

    #     if response.status_code == 200:
    #         print("Task status retrieved successfully!")
    #     else:
    #         print(f"Error: {response.status_code}. {response.text}")

    # Delete files from local directory
    # shutil.rmtree(directory)
    # os.remove(csv_file)
                                
    return "success"

def downloadPDFSpringer(record: dict, directory: str):
    """
    This function takes a record from the Springer API response and downloads the PDF file.
    It is called in a multi-process loop in downloadSpringerFulltext().
    Args:
        record: dictionary containing DOI and other metadata
        directory: local directory to save the files
    """
    print("in downloadPDFSpringer")
    headers = {'Accept': 'application/json'}

    if len(record['url']) < 1:
        return "No download link found for DOI: " + record['doi']

    # extract URL
    url = record['url'][0]['value'] + "?api_key=" + str(SPRINGER_API_KEY)
    print("URL: ", url)
    url_response = requests.get(url, headers=headers)
    print("URL response: ", url_response.status_code)
    if url_response.status_code != 200:
        return "Error in accessing article link: " + str(url_response.status_code) + " - " + url_response.text
    
    url_data = url_response.json()

    if 'license' in url_data:
        license_url = url_data['license'][0]['URL']
        license = CC_LICENSES.get(license_url, license_url)
        print("License: ", license)
    else:
        license = "unknown"
        license_url = "unknown"

    # extract PDF link
    pdf_link = None
    links = url_data['link']
    for link in links:
        if link['content-type'] == 'application/pdf' and link['intended-application'] == 'text-mining':
            pdf_link = link['URL']
            print("PDF Link: ", pdf_link)
            break
        
    if not pdf_link:
        pdf_link = links[0]['URL']
        print("PDF Link: ", pdf_link)
        if not pdf_link:
            return "No PDF link found for DOI: " + record['doi']
    
    # download PDF
    print("Downloading PDF: ", record['doi'])
    if 'doi' in record:
        filename = record['doi'].replace("/", "_")
    else:
        filename = url_data['DOI'].replace("/", "_")

    try:
        response = requests.get(pdf_link)
        if response.status_code != 200:
            return "Error in downloading PDF: " + str(response.status_code) + " - " + response.text
        
        with open(directory + "/" + filename + ".pdf", "wb") as f:  # Open a file in binary write mode ("wb")
            for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
                f.write(chunk)
        print("Downloaded: ", filename)

        # form metadata
        metadata = {
            "doi": record['doi'],
            "publisher": record['publisher'],
            "issn": record['issn'],
            "license": license,
            "license_url": license_url,
            "metadata": url_data,
        }
        return metadata
    except Exception as e:
        return "Error in downloading PDF: " + str(e)

def getCrossrefMetadata(issn: str):
    """
    Creates a csv file with metadata of all articles for given journal (ISSN)
    """
    try:
        metadata = []
        # get journal metadata
        journals = Journals()
        works = journals.works(issn=issn)
        count = 0
        no_license = 0
        for item in works:
            try:
                count += 1
                article_metadata = {}
                # check if the license is open access - variant of CC
                if 'license' not in item:
                    no_license += 1
                    continue
                else:
                    for license in item['license']:
                        # check for creative commons license
                        if license['URL'] in CC_LICENSES:
                            article_metadata['license'] = CC_LICENSES[license['URL']]
                            article_metadata['license_url'] = license['URL']
                            break
                        elif license['URL'] in OTHER_LICENSES:
                            article_metadata['license'] = OTHER_LICENSES[license['URL']]
                            article_metadata['license_url'] = license['URL']
                        else:
                            article_metadata['license'] = "unknown"
                            article_metadata['license_url'] = license['URL']
                            
                article_metadata['doi'] = item['DOI']
                if 'title' not in item:
                    article_metadata['title'] = "No title found"
                else:
                    article_metadata['title'] = item['title'][0]
                article_metadata['journal'] = item['container-title'][0]
                article_metadata['publisher'] = item['publisher']
                article_metadata['issn'] = item['ISSN'][0]
                article_metadata['url'] = item['URL']
                article_metadata['filename'] = item['DOI'].replace("/", "_") + ".pdf"
                article_metadata['downloaded'] = "no"
                metadata_csv = "wiley_metadata.csv"
                metadata_df = pd.DataFrame([article_metadata])
                if not os.path.exists(metadata_csv):
                    metadata_df.to_csv(metadata_csv, index=False)
                else:
                    metadata_df.to_csv(metadata_csv, mode='a', header=False, index=False)
                #metadata.append(article_metadata)
                print("Processed: ", article_metadata['doi'])
            except Exception as e:
                print("Error processing article: ", article_metadata['doi'], e)
        print("Total articles: ", count)
        # metadata_csv = "wiley_metadata.csv"
        # metadata_df = pd.DataFrame(metadata)
        # if not os.path.exists(metadata_csv):
        #     metadata_df.to_csv(metadata_csv, index=False)
        # else:
        #     metadata_df.to_csv(metadata_csv, mode='a', header=False, index=False)
        
        return "success"
    except Exception as e:
        return "Error: " + str(e)


def downloadWileyFulltext(course_name=None, issn=[]):
    """
    This function fetches metadata from Crossref and downloads 
    full-text articles from a given journal from Wiley.
    """
    # create directory to store files
    directory = os.path.join(os.getcwd(), 'wiley_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)


    # fetch metadata
    for item in issn:
        metadata_status = getCrossrefMetadata(item)
        print("Metadata status: ", metadata_status)
    
    # download PDFs based on metadata
    metadata_csv = "wiley_metadata.csv"
    if os.path.exists(metadata_csv):
        metadata_df = pd.read_csv(metadata_csv)
        metadata = metadata_df.to_dict(orient='records')

    for item in metadata:
        try:
            if item['license'] in ['CC BY', 'CC BY-NC', 'CC BY-NC-ND', 'CC BY-NC-SA'] and item['downloaded'] == 'no' and item['publisher'] == 'Wiley':
                status = downloadWileyPDF(item['doi'])
                print("Download status: ", status)
                if status == "success":
                    item['downloaded'] = 'yes'
                time.sleep(5)    
        except Exception as e:
            print(e)
        
        #time.sleep(10)
    
    metadata_df = pd.DataFrame(metadata)
    metadata_df.to_csv(metadata_csv, index=False) 

    return "success"

    
    # # prep payload for beam ingest
    # ingest_data = []
        
    # # upload files to S3 bucket
    # for file in os.listdir(directory):
    #     doi = file[:-4]
    #     doi = doi.replace("_", "/")
    #     doi_link = f"https://doi.org/{doi}"
    #     data = {
    #         "course_name": course_name,
    #         "group": "wiley",
    #         "s3_paths": "courses/" + course_name + "/" + file, # type: ignore
    #         "readable_filename": file,
    #         "base_url": "",
    #         "url": doi_link,
    #         "journal": "",
    #     }
    #     s3_path = "courses/" + course_name + "/" + file # type: ignore
    #     s3_client.upload_file(directory + "/" + file, aws_bucket, s3_path)  # type: ignore
    #     ingest_data.append(data)
        
    # # save ingest data to csv
    # ingest_df = pd.DataFrame(ingest_data)
    # csv_file = "publications_data.csv"
    # if not os.path.exists(csv_file):
    #     ingest_df.to_csv(csv_file, index=False)
    # else:
    #     ingest_df.to_csv(csv_file, mode='a', header=False, index=False)


    # # call ingest
    # beam_url = "https://41kgx.apps.beam.cloud"
    # headers = {
    # "Content-Type": "application/json",
    # "Authorization": "Basic " + os.getenv('BEAM_AUTH_TOKEN')    # type: ignore
    # }
    # for data in ingest_data:
    #     payload = json.dumps(data)
    #     response = requests.post(beam_url, headers=headers, data=payload)
    #     if response.status_code == 200:
    #         print("Task status retrieved successfully!")
    #     else:
    #         print(f"Error: {response.status_code}. {response.text}")

    # Delete files from local directory
    #shutil.rmtree(directory)

    
                
#@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=1)
def downloadWileyPDF(doi=None):
    """
    This function downloads a PDF file from Wiley based on the DOI.
    """
    try:
        # create directory to store files
        directory = os.path.join(os.getcwd(), 'wiley_papers')
        if not os.path.exists(directory):
            os.makedirs(directory)

        api_key = os.environ.get("WILEY_TDM_TOKEN")

        # download PDF based on doi
        base_url = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
        url = base_url + str(doi)
        print("URL: ", url)

        headers = {
            'Wiley-TDM-Client-Token': api_key,
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
            
        filename = str(doi).replace("/", "_") + ".pdf"
        with open(directory + "/" + filename, "wb") as f:  # Open a file in binary write mode ("wb")
            for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
                f.write(chunk)
        print("Downloaded: ", filename)
        
        return "success"
    except Exception as e:
        print("Error: ", e)
        return "error"


def downloadWileyArticle(doi=None):
    """
    This function fetches metadata from Crossref and downloads open access full text articles from Wiley.
    """
    # create directory to store files
    directory = os.path.join(os.getcwd(), 'wiley_papers')
    if not os.path.exists(directory):
        os.makedirs(directory)

    api_key = os.environ.get("WILEY_TDM_TOKEN")
    metadata = {}
    
    # get metadata from Crossref
    if doi:
        # get article metadata
        works = Works()
        article_data = works.doi(doi)
        print("Article license: ", article_data['license'])
        
        article_licenses = []
        
        for item in article_data['license']:
            article_licenses.append(item['URL'])
        print("Licenses: ", article_licenses)
        # check if the license is open access - variant of CC
        for license in article_licenses:
            if license in LICENSES:
                print("License found: ", license)
                if LICENSES[license] == "closed_access":
                    return "Article is not open access."
                else:
                    metadata['license'] = LICENSES[license]
                    break
            else:
                return "License not found."
        
        metadata['doi'] = doi
        metadata['title'] = article_data['title'][0]
        metadata['journal'] = article_data['container-title'][0]
        metadata['publisher'] = article_data['publisher']
        metadata['issn'] = article_data['ISSN'][0]
        metadata['url'] = article_data['URL']

        print("Metadata: ", metadata)

        # download PDF based on doi
        base_url = "https://api.wiley.com/onlinelibrary/tdm/v1/articles/"
        url = base_url + str(doi)

        print("URL: ", url)

        headers = {
            'Wiley-TDM-Client-Token': api_key,
            'Content-Type': 'application/json'
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            # exponential backoff logic
            print("Error in accessing article link, retrying: ", response.text)

            return "Error in accessing article link: " + str(response.status_code) + " - " + response.text
        
        filename = str(doi).replace("/", "_")
        with open(directory + "/" + filename + ".pdf", "wb") as f:  # Open a file in binary write mode ("wb")
            for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
                f.write(chunk)
        print("Downloaded: ", filename)

        # upload file to S3 bucket

        # prep payload for beam ingest

        return "success"