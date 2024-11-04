import os
import json
from openai import OpenAI
from sentence_transformers import SentenceTransformer, util
import supabase
import concurrent.futures
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

# Create a new client
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_API_KEY')
SUPABASE_CLIENT = supabase.create_client(supabase_url, supabase_key)
MODEL = SentenceTransformer('paraphrase-MiniLM-L6-v2')
OPENAI_CLIENT = OpenAI(api_key=os.getenv('AIFARMS_OPENAI_API_KEY'))
MINIO_CLIENT = Minio(os.environ['MINIO_URL'],
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=True
)


# Define the maximum context window size for GPT-4o-mini
MAX_CONTEXT_LENGTH = 8000  # Adjust this value based on the actual limit

def split_text(text, max_length):
    """Split the text into chunks of maximum length."""
    chunks = []
    current_chunk = ""
    for sentence in text.split(". "):
        if len(current_chunk) + len(sentence) < max_length:
            current_chunk += sentence + ". "
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

def get_keywords(prompt):
    completion = OPENAI_CLIENT.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": prompt
            }
        ]
    )
    list_str = completion.choices[0].message.content.strip()
    if list_str and list_str[-1] == ".":
        list_str = list_str[:-1]
    word_list = list_str.split(", ")
    return word_list

def compute_semantic_similarity(keywords, text):
    text_embedding = MODEL.encode(text, convert_to_tensor=True)
    similarity_scores = {}
    for keyword in keywords:
        keyword_embedding = MODEL.encode(keyword, convert_to_tensor=True)
        cosine_sim = util.pytorch_cos_sim(keyword_embedding, text_embedding).item()
        similarity_scores[keyword] = round(cosine_sim, 4)
    return similarity_scores

import torch
from sentence_transformers import SentenceTransformer, util

# Assume MODEL is already instantiated with SentenceTransformer('paraphrase-MiniLM-L6-v2')

def compute_semantic_similarity_batch(keywords, text):
    """
    Compute cosine similarity between the text and all the keywords using batch processing.
    """
    # Encode the entire text and all keywords in one go
    # These will be batches of embeddings
    text_embedding = MODEL.encode(text, convert_to_tensor=True)  # Single text embedding
    keyword_embeddings = MODEL.encode(keywords, convert_to_tensor=True)  # Batch of keyword embeddings
    
    # Compute pairwise cosine similarities between text embedding and keyword embeddings
    cosine_similarities = util.pytorch_cos_sim(keyword_embeddings, text_embedding).squeeze()  # Shape: (num_keywords,)
    
    # Create a dictionary of keywords and their corresponding similarity scores
    similarity_scores = {}
    for i, keyword in enumerate(keywords):
        similarity_scores[keyword] = round(cosine_similarities[i].item(), 4)  # Convert tensor to float
    
    return similarity_scores


def generateKeywords(full_text):
    chunks = split_text(full_text, MAX_CONTEXT_LENGTH)
    all_keywords = {}
    
    for i, chunk in enumerate(chunks):
        prompt = """You are an expert in keyword extraction in plant science and agricultural domain. From the text provided, 
            extract 50 unique keywords most relevant to the main topics and themes. Pick keywords that are actually present 
            in the text. Do not generate keywords on your own. If the text contains a "Keywords" section, extract those words too. Rank the keywords from the most specific 
            to general. Return the keywords as a comma-separated string, with no extra formatting or explanation.

            Text:
            {paper}
            """
        prompt = prompt.format(paper=chunk)
        keywords = get_keywords(prompt)
        semantic_similarity_scores = compute_semantic_similarity_batch(keywords, chunk)
        
        for keyword, score in semantic_similarity_scores.items():
            if keyword in all_keywords:
                all_keywords[keyword] = max(all_keywords[keyword], score)
            else:
                all_keywords[keyword] = score
    
    return all_keywords

def upload_file(client, bucket_name, file_path, object_name, error_file, upload_log):
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded: {object_name}")
        with open(upload_log, 'a') as f:
            f.write("uploaded: " + file_path + "\n")
        os.remove(file_path)
    except Exception as e:
        with open(error_file, 'a') as f:
            f.write("Error in upload_file(): " + str(e) + "\n")

def uploadToStorage(filepath: str, error_file: str):
    try:
        bucket_name = "cropwizard-keywords"
        found = MINIO_CLIENT.bucket_exists(bucket_name)
        if not found:
            MINIO_CLIENT.make_bucket(bucket_name)
            print("Created bucket", bucket_name)
        upload_log = error_file.split("_")[0] + ".txt"
        files = []
        for root, _, files_ in os.walk(filepath):
            for file in files_:
                file_path = os.path.join(root, file)
                object_name = file_path.split("/")[-1]
                files.append((MINIO_CLIENT, bucket_name, file_path, object_name, error_file, upload_log))

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(0, len(files), 10):
                batch_files = files[i:i+10]
                futures = [executor.submit(upload_file, *args) for args in batch_files]
                done, not_done = concurrent.futures.wait(futures, timeout=180)

                for future in not_done:
                    future.cancel()
                
                for future in done:
                    try:
                        future.result()
                    except Exception as e:
                        with open(error_file, 'a') as f:
                            f.write("Error in upload_file(): " + str(e) + "\n")

        return "success"
    except Exception as e:
        with open(error_file, 'a') as f:
            f.write("Error in uploadToStorage(): " + str(e) + "\n")
        return "failure"

def main():
    total_count = 411871
    current_count = 24200
    last_id = 140536
    iteration = 0

    print(os.getenv('AIFARMS_OPENAI_API_KEY'))
    #exit()

    keywords_folder = "/home/asmita/ai-ta-backend/ai_ta_backend/keywords"
    error_file = "errors.txt"
    result = uploadToStorage(keywords_folder, error_file)

    while current_count < total_count:
        response = SUPABASE_CLIENT.table("documents").select("id, contexts").eq("course_name", "cropwizard-1.5").gt("id", last_id).order("id", desc=False).limit(100).execute()
        data = response.data

        last_id = data[-1]["id"]
        print("last id: ", last_id)

        current_count += len(data)
        iteration += 1

        print("current_count: ", current_count)

        # write count and id to file
        with open("data_log.txt", "a") as f:
            f.write(f"Current count: {current_count}, last ID: {last_id}")

        if current_count <= 24082:
            continue

        for doc in data:
            try:
                doc_string = ""
                for context in doc['contexts']:
                    doc_string += context["text"] + " "

                result = generateKeywords(doc_string)

                filename = f"/home/asmita/ai-ta-backend/ai_ta_backend/keywords/keywords_{doc['id']}.txt"
                with open(filename, "a", encoding="utf-8") as f:
                    for keyword, score in result.items():
                        f.write(f"{keyword}: {score}\n")
            except Exception as e:
                print("Error: ", e)
    
        print("last id: ", last_id)
        print("current count: ", current_count)

        result = uploadToStorage(keywords_folder, error_file)

if __name__ == "__main__":
    main()