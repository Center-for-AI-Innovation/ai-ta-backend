import math
import os
import tempfile
import time
import traceback
from typing import Any, Dict

import sentry_sdk
import tiktoken
from doc2json.tei_to_json import convert_tei_xml_file_to_s2orc_json
from dotenv import load_dotenv
from embedding import get_embeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter  # type: ignore
from posthog import Posthog

BASE_TEMP_DIR = 'temp'
BASE_OUTPUT_DIR = 'output'

load_dotenv(override=True)

posthog = Posthog(sync_mode=True,
                  project_api_key=os.environ['LLM_GUIDED_RETRIEVAL_POSTHOG_API_KEY'],
                  host='https://us.i.posthog.com')

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)


def process_pdf_file(
    input_file: os.PathLike,
    temp_dir: str = BASE_TEMP_DIR,
    output_dir: str = BASE_OUTPUT_DIR,
    minio_path: str = '',
    grobidClient: Any = None,
) -> Dict | None:
  """
  Process a PDF file and get JSON representation
  :param input_file:
  :param temp_dir:
  :param output_dir:
  :return:
  """

  tei_file = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False, suffix='.tei.xml')
  output_file = tempfile.NamedTemporaryFile(dir=output_dir, delete=False, suffix='.json')

  try:
    start_time = time.monotonic()
    grobidClient.process_pdf(str(input_file), tei_file.name, "processFulltextDocument")
    # print(f"📜 Grobid Runtime: {(time.monotonic() - start_time):.2f} seconds")
    posthog.capture('llm-guided-ingest',
                    event='grobid_runtime_v2',
                    properties={
                        'runtime_sec': float(f"{(time.monotonic() - start_time):.2f}"),
                        'minio_file': f'{str(minio_path)}',
                        'grobid_using_GPU': True,
                    })

    assert os.path.exists(tei_file.name)

    paper = convert_tei_xml_file_to_s2orc_json(tei_file.name)
    output_data = paper.release_json()
    return output_data
  except Exception as e:
    print(f"Error in process_pdf_file(): {str(e)}")
    traceback.print_exc()
    raise ValueError(f"Error in process_pdf_file(): {str(e)}")
  finally:
    if tei_file.name and os.path.exists(tei_file.name):
      os.remove(tei_file.name)
    if os.path.exists(output_file.name):
      os.remove(output_file.name)


def parse_and_group_by_section(data) -> Any:
  """
    This parses the output of AllenAI's Grobid wrapper. https://github.com/allenai/s2orc-doc2json

    Output format is two dictionaries: 
    * Full text grouped by major section. 
    * The "outline:" all the section titles, including minor sections. This might be useful as a separate embedding. 
    """
  grouped_texts = {}
  grouped_titles = {}
  grouped_pages = {}
  all_sections = {}
  references = {}
  ref_num_tokens = {}
  failed_secs = 0

  # TODO: I think authors and title are maybe not being handled perfectly when data is missing...
  metadata = {
      "title":
          data["title"] if data and "title" in data else None,
      "authors":
          None if not data or not data.get("authors") else [
              " ".join(filter(None, [author.get("first"), " ".join(author.get("middle", [])),
                                     author.get("last")])) for author in data.get("authors", [])
          ],
      "date_published":
          data["year"] if data and "year" in data else None,
      "journal":
          data["venue"] if data and "venue" in data else None,
      "paper_id":
          data["paper_id"] if data and "paper_id" in data else None,
      "header":
          data["header"] if data and "header" in data else None,
      "year":
          data["year"] if data and "year" in data else None,
      "identifiers":
          data["identifiers"] if data and "identifiers" in data else None,
      "abstract":
          data["abstract"] if data and "abstract" in data else None,
  }

  try:
    encoding = tiktoken.get_encoding("cl100k_base")

    for key in data["pdf_parse"]["bib_entries"]:
      references[key] = format_reference(data["pdf_parse"]["bib_entries"][key])
      ref_num_tokens[key] = len(encoding.encode(references[key]))

    if metadata['abstract']:
      grouped_texts['0'] = metadata['abstract']
      grouped_titles['0'] = 'Abstract'

    null_sec = 1
    cur_sec = null_sec
    # for entry in data["pdf_parse"]["body_text"]:
    #   print(f"Entry: {entry}")

    for entry in data["pdf_parse"]["body_text"]:
      text = entry["text"]
      section = entry["section"]
      sec_num = entry["sec_num"]
      # page_num = entry["page_num"]
      try:
        major_sec_num = math.floor(float(sec_num))
      except Exception:
        major_sec_num = -1
        failed_secs += 1
        # print(f"Failed: {failed_secs}", json.dumps(entry, indent=2))

      if not sec_num and section not in grouped_titles.values():
        sec_num = str(null_sec)
        cur_sec = str(null_sec)
        null_sec += 1

      if sec_num:
        if sec_num in grouped_texts:
          grouped_texts[sec_num] += " " + text
          # grouped_pages[sec_num].append(page_num)
        else:
          grouped_texts[sec_num] = text
          grouped_titles[sec_num] = section
          # grouped_pages[sec_num] = [page_num]
      else:
        grouped_texts[cur_sec] += " " + text

      if sec_num:
        all_sections[sec_num] = section

    if not all_sections:
      num = 1
      for i in data["pdf_parse"]["body_text"]:
        grouped_texts[str(num)] = i["text"]
        num += 1

    text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        model_name="gpt-4",
        chunk_size=7000,
        chunk_overlap=400,
    )

    valid_keys = [key for key in grouped_texts if key is not None]

    result = []

    for key in sorted(valid_keys):
      section_text = grouped_texts[key]
      section_title = grouped_titles.get(key, None)
      # section_page = grouped_pages.get(key, "Unknown")
      tokens = len(encoding.encode(section_text))

      result.append({
          "text": section_text,
          "sec_num": key,
          "sec_title": section_title,
          "tokens": tokens,
          # "page_num": section_page,
          "chunk_text": [],
          "chunk_tokens": [],
          "embedding": []
      })

      if tokens > 7000:
        chunks = text_splitter.split_text(section_text)
        for i, chunk in enumerate(chunks):
          chunk_tokens = len(encoding.encode(chunk))
          result[-1]["chunk_text"].append(chunk)
          result[-1]["chunk_tokens"].append(chunk_tokens)
      else:
        result[-1]["chunk_text"].append(section_text)
        result[-1]["chunk_tokens"].append(tokens)

      # print("start embedding")
      for i, chunk in enumerate(result[-1]["chunk_text"]):
        context_with_title = f'Section {result[-1]["sec_num"]}: {result[-1]["sec_title"]}\n{chunk}'
        embedding = get_embeddings(context_with_title)
        result[-1]["embedding"].append(embedding)

    total_tokens = sum([entry["tokens"] for entry in result])
    return metadata, result, total_tokens, references, ref_num_tokens

  except Exception as e:
    sentry_sdk.capture_exception(e)
    raise (ValueError(f"Failed parse_and_grou_by_section() with error: {e}"))


def format_reference(reference):
  # Extract individual fields from the reference
  # ref_id = reference['ref_id']
  # title = reference.get('title', '')
  # authors = reference.get('authors', [])
  # year = reference.get('year', '')
  # venue = reference.get('venue', '')
  # volume = reference.get('volume', '')
  # issue = reference.get('issue', '')
  # pages = reference.get('pages', '')
  # other_ids = reference.get('other_ids', {})
  # num = reference.get('num', '')
  urls = reference.get('urls', [])
  raw_text = reference.get('raw_text', '')
  links = reference.get('links', '')

  combined_text = (f"{raw_text}. {urls}. {links}")

  return combined_text
