import os
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Manager

DOCUMENTS_TABLE = os.environ['SUPABASE_DOCUMENTS_TABLE']
# SUPABASE_CLIENT = supabase.create_client(supabase_url=os.environ['SUPABASE_URL'],
#  supabase_key=os.environ['SUPABASE_API_KEY'])  # type: ignore


def context_parent_doc_padding(found_docs, search_query, course_name):
  """
    Takes top N contexts acquired from QRANT similarity search and pads them
    """
  print("inside main context padding")
  start_time = time.monotonic()

  with Manager() as manager:
    qdrant_contexts = manager.list()
    supabase_contexts = manager.list()
    partial_func1 = partial(qdrant_context_processing, course_name=course_name, result_contexts=qdrant_contexts)
    partial_func2 = partial(supabase_context_padding, course_name=course_name, result_docs=supabase_contexts)

    with ProcessPoolExecutor() as executor:
      executor.map(partial_func1, found_docs[5:])
      executor.map(partial_func2, found_docs[:5])

    supabase_contexts_no_duplicates = []
    for context in supabase_contexts:
      if context not in supabase_contexts_no_duplicates:
        supabase_contexts_no_duplicates.append(context)

    result_contexts = supabase_contexts_no_duplicates + list(qdrant_contexts)

    print(f"⏰ Context padding runtime: {(time.monotonic() - start_time):.2f} seconds")

    return result_contexts


def qdrant_context_processing(doc, course_name, result_contexts):
  """
    Re-factor QDRANT objects into Supabase objects and append to result_docs
    """
  context_dict = {
      'text': doc.page_content,
      'embedding': '',
      'pagenumber': doc.metadata['pagenumber'],
      'readable_filename': doc.metadata['readable_filename'],
      'course_name': course_name,
      's3_path': doc.metadata['s3_path'],
      'base_url': doc.metadata['base_url']
  }
  if 'url' in doc.metadata.keys():
    context_dict['url'] = doc.metadata['url']
  else:
    context_dict['url'] = ''

  result_contexts.append(context_dict)
  return result_contexts


def supabase_context_padding(doc, course_name, result_docs):
  """
    Does context padding for given doc.
    """

  # query by url or s3_path
  if 'url' in doc.metadata.keys() and doc.metadata['url']:
    parent_doc_id = doc.metadata['url']
    response = SUPABASE_CLIENT.table(DOCUMENTS_TABLE).select('*').eq('course_name',
                                                                     course_name).eq('url', parent_doc_id).execute()

  else:
    parent_doc_id = doc.metadata['s3_path']
    response = SUPABASE_CLIENT.table(DOCUMENTS_TABLE).select('*').eq('course_name',
                                                                     course_name).eq('s3_path',
                                                                                     parent_doc_id).execute()

  data = response.data

  if len(data) > 0:
    # do the padding
    filename = data[0]['readable_filename']
    contexts = data[0]['contexts']
    #print("no of contexts within the og doc: ", len(contexts))

    if 'chunk_index' in doc.metadata and 'chunk_index' in contexts[0].keys():
      #print("inside chunk index")
      # pad contexts by chunk index + 3 and - 3
      target_chunk_index = doc.metadata['chunk_index']
      for context in contexts:
        curr_chunk_index = context['chunk_index']
        if (target_chunk_index - 3 <= curr_chunk_index <= target_chunk_index + 3):
          context['readable_filename'] = filename
          context['course_name'] = course_name
          context['s3_path'] = data[0]['s3_path']
          context['url'] = data[0]['url']
          context['base_url'] = data[0]['base_url']
          result_docs.append(context)

    elif doc.metadata['pagenumber'] != '':
      #print("inside page number")
      # pad contexts belonging to same page number
      pagenumber = doc.metadata['pagenumber']

      for context in contexts:
        # pad contexts belonging to same page number
        if int(context['pagenumber']) == pagenumber:
          context['readable_filename'] = filename
          context['course_name'] = course_name
          context['s3_path'] = data[0]['s3_path']
          context['url'] = data[0]['url']
          context['base_url'] = data[0]['base_url']
          result_docs.append(context)

    else:
      #print("inside else")
      # refactor as a Supabase object and append
      context_dict = {
          'text': doc.page_content,
          'embedding': '',
          'pagenumber': doc.metadata['pagenumber'],
          'readable_filename': doc.metadata['readable_filename'],
          'course_name': course_name,
          's3_path': doc.metadata['s3_path'],
          'base_url': doc.metadata['base_url']
      }
      if 'url' in doc.metadata.keys():
        context_dict['url'] = doc.metadata['url']
      else:
        context_dict['url'] = ''

      result_docs.append(context_dict)
