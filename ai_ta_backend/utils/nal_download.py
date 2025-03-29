import os
from supabase import create_client, Client
import requests
import boto3
from dotenv import load_dotenv
import datetime
import time

load_dotenv()
print("Supabase URL: ", os.getenv("SUPABASE_URL"))
print("Supabase API key: ", os.getenv("SUPABASE_API_KEY"))

# Initialize the Supabase client
SUPABASE_CLIENT: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_API_KEY"))
SPRINGER_API_KEY = os.environ.get('SPRINGER_API_KEY')
DOWNLOAD_LOG = "download_log.txt"

S3_CLIENT = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

AWS_BUCKET = os.getenv('S3_BUCKET_NAME')

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


def main():
    data = [1, 2, 3]
    # fetch records from SQL
    while len(data) > 0:
        response = SUPABASE_CLIENT.table("nal_publications").select("doi_number, publisher, metadata").eq("ingested", False).eq("downloadable", True).eq("publisher", "Wiley").limit(5).execute()
        data = response.data
        print("No. of records: ", len(data))
        for record in data:
            if 'Springer' in record['publisher']:
                # route to springer download
                print('Routing to Springer...')
                result = downloadSpringerFulltext(doi=record['doi_number'])
                
            elif 'Wiley' in record['publisher']:
                # route to wiley download
                print('Routing to Wiley...')
                result = downloadWileyPDF(doi=record['doi_number'], metadata=record['metadata'])
                time.sleep(10) # sleep for 10 seconds to avoid rate limiting
            elif 'Elsevier' in record['publisher']:
                # update supabase
                print('Routing to Elsevier...')
                update_info = {"notes": "Elsevier articles not downloadable.", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
                response = SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", record['doi_number']).execute()

            else:
                # regular file save
                print("publisher name: " , record['publisher'])
                result = download_article_from_url(record['doi_number'], record['metadata'])
                print(result)
                time.sleep(10) # sleep for 10 seconds to avoid rate limiting
        break
    return "Success"


def download_article_from_url(doi, metadata):    
    print("in download_article_from_url: ", doi)
    
    if 'link' not in metadata:
        print("No link")
        # update supabase
        update_info = {"notes": "Download link absent.", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
        SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()
        return "No download link present"
    else:
        # save to local
        print("Link found")
        pdf_link = metadata['link'][0]['URL']

        if 'license' not in metadata:
            print("No license")
            # update supabase
            update_info = {"notes": "License absent.", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
            SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()
            return {"error": "License not found."}

        license = get_license(metadata['license'][0]['URL'])

        status = download_pdf_in_chunks(url=pdf_link, doi=doi)
        if 'failed' in status:
            # update supabase
            print("Error in PDF download: ", status['failed'])
            update_info = {"notes": str(status['failed']), "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
            SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()
            return {"error": "Error in PDF download."}
        else:
            filepath = status['success']

            updated_metadata = {
                "doi": doi,
                "filename": filepath.split("/")[-1],
                "file_path": filepath,
                "publisher": metadata['publisher'],
                "license": license,
            }
            print("Updated metadata: ", updated_metadata)
            
            ingest_status = upload_and_ingest(filepath, updated_metadata, doi)
            print(ingest_status)

            return {"success": "Downloaded and ingested successfully."}

def upload_and_ingest(filepath, metadata, doi):
    """
    Uploads file to S3 and ingests them into cropwizard-1.5
    """
    filename = os.path.basename(filepath)

    s3_path = "courses/cropwizard-1.5/" + filename

    S3_CLIENT.upload_file(filepath, AWS_BUCKET, s3_path)
    
    publisher = metadata['publisher']
    if 'Springer' in publisher:
        publisher = "Springer"
    elif 'Wiley' in publisher:
        publisher = "Wiley"

    
    # ingest
    ingest_url = "https://ingest-task-queue-6ee4a59-v29.app.beam.cloud" # TODO: change to cropwizard-ingest endpoint -> https://cw-ingest-task-queue-3df80f3-v2.app.beam.cloud
    ingest_headers = {
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate',
          'Authorization': f"Bearer {os.environ['BEAM_API_KEY']}",
          'Content-Type': 'application/json',
    }
    doi_url = f"https://doi.org/{doi}"
    ingest_payload = {
        "course_name": "cropwizard-1.5",
        "s3_paths": [s3_path],
        "readable_filename": filename,
        "url": doi_url,
        "base_url": "",
        "groups": ["Research Papers", "NAL", publisher]
    }
    
    if 'license' in metadata and metadata['license'] not in ['Unknown', 'unknown']:
        ingest_payload['groups'].append(metadata['license'])

    print("FINAL INGEST PAYLOAD: ", ingest_payload)
    ingest_response = requests.post(ingest_url, headers=ingest_headers, json=ingest_payload)

    # update supabase
    update_info = {"ingested": True, "modified_date": datetime.datetime.now().isoformat()}
    response = SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()
    print("Updated supabase: ", response)
    return "success"


def download_pdf_in_chunks(url, doi, chunk_size=1024):
    try:
        
        # create directory to store files
        directory = "other_papers"
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Send a GET request to the URL with stream=True to download in chunks
        response = requests.get(url, stream=True)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Open the file in binary write mode
            filename = doi.replace("/", "_")
            filepath = "other_papers/" + filename + ".pdf"
            with open(filepath, 'wb') as file:
                # Iterate over the response in chunks and write each to the file
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
            print(f"PDF successfully downloaded and saved as {filepath}")

            return {"success": filepath}

        else:
            print(f"Failed to download PDF. Status code: {response.status_code}")

            # update supabase
            update_info = {"notes": f"Failed to download PDF (anti-bot). Status code: {response.status_code}",
                           "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
            SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()

            return {"failed": response.status_code}
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return {"failed": e}
    

############# SPRINGER DOWNLOAD #############

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

    if response.status_code != 200:
        print("Error in accessing Springer API: ", response.text)
        response = SUPABASE_CLIENT.table("nal_publications").update({"notes": f"Error in accessing Springer API. Status code: {response.text}", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}).eq("doi_number", doi).execute()
        return "Error" 

    data = response.json()
    # check for total number of records 
    total_records = int(data['result'][0]['total'])

    if total_records == 0:
        # update supabase record and exit
        response = SUPABASE_CLIENT.table("nal_publications").update({"notes": "Article is not OA.", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}).eq("doi_number", doi).execute()
        return "Article not OA."
    else:
        # download paper
        download_info = downloadPDFSpringer(data['records'][0], directory)
        
        if 'error' in download_info:
            response = SUPABASE_CLIENT.table("nal_publications").update({"notes": download_info['error'], "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}).eq("doi_number", doi).execute()
        else:
            # ingest
            print("Download info: ", download_info)
            ingest_status = upload_and_ingest(download_info['file_path'], download_info, doi)
                                
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
    
    url_response = requests.get(url, headers=headers)
    
    if url_response.status_code != 200:
        return {"error": "Error in accessing article link: " + str(url_response.status_code) + " - " + url_response.text}
    
    url_data = url_response.json()

    if 'license' in url_data:
        license_url = url_data['license'][0]['URL']
        license = get_license(license_url)
        print("License: ", license)
    else:
        license = "unknown"
        license_url = "unknown"

    # extract PDF link
    pdf_link = None
    if 'link' not in url_data:
        return {"error": "No link found for DOI: " + record['doi']}
    
    links = url_data['link']
    for link in links:
        if link['content-type'] == 'application/pdf' and link['intended-application'] == 'text-mining':
            pdf_link = link['URL']
            
            break
        
    if not pdf_link:
        pdf_link = links[0]['URL']
        
        if not pdf_link:
            return {"error": "No PDF link found for DOI: " + record['doi']}
    
    # download PDF
    
    if 'doi' in record:
        filename = record['doi'].replace("/", "_")
    else:
        filename = url_data['DOI'].replace("/", "_")

    try:
        response = requests.get(pdf_link)
        if response.status_code != 200:
            return {"error": "Error in downloading PDF: " + str(response.status_code) + " - " + response.text}
        
        with open(directory + "/" + filename + ".pdf", "wb") as f:  # Open a file in binary write mode ("wb")
            for chunk in response.iter_content(chunk_size=1024):  # Download in chunks
                f.write(chunk)
        

        # form metadata
        metadata = {
            "doi": record['doi'],
            "publisher": record['publisher'],
            "issn": record['issn'],
            "license": license,
            "license_url": license_url,
            "filename": filename + ".pdf",
            "file_path": directory + "/" + filename + ".pdf"
        }
        return metadata
    except Exception as e:
        return {"error": "Error in downloading PDF: " + str(e)}


def downloadWileyPDF(doi, metadata):
    """
    This function downloads a PDF file from Wiley based on the DOI.
    """
    print("in downloadWileyPDF")
    try:
        # create directory to store files
        directory = "wiley_papers"
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

        # get license
        license = get_license(metadata['license'][0]['URL'])
        print("License: ", license)

        # route to upload and ingest
        updated_metadata = {
            "doi": doi,
            "filename": filename,
            "file_path": directory + "/" + filename,
            "publisher": metadata['publisher'],
            "license": license,
        }
        print("Updated metadata: ", updated_metadata)
        
        # call upload and ingest
        ingest_status = upload_and_ingest(updated_metadata['file_path'], updated_metadata, doi)
        
        return {"success": "Downloaded and ingested successfully."}
    except Exception as e:
        print("Error: ", e)
        # probably a 403 error - update supabase
        update_info = {"notes": "403 client error (forbidden) in PDF download.", "downloadable": False, "modified_date": datetime.datetime.now().isoformat()}
        response = SUPABASE_CLIENT.table("nal_publications").update(update_info).eq("doi_number", doi).execute()
        return {"error": "403 client error (forbidden) in PDF download."}


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


def get_license(url: str) -> str:
    # Define license matches
    license_mapping = {
        "by-nc-nd": "CC BY-NC-ND",
        "by-nc-sa": "CC BY-NC-SA",
        "by-nc": "CC BY-NC",
        "by": "CC BY",
    }
    
    # Loop through the mapping and check if the URL contains the license string
    for key, license in license_mapping.items():
        if key in url:
            return license

    # Return 'Unknown' if no match is found
    return "Unknown"

if __name__ == "__main__":
    main()