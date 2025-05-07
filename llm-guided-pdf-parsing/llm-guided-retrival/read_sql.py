import sqlite3

db = '/home/guest/ai-ta-backend/UIUC_Chat/pdf-parsing/articles-test.db'


def get_context_given_contextID(context_id):
  conn = sqlite3.connect(db)
  cursor = conn.cursor()

  query_article_id = """
    SELECT articles.ID, articles.Outline
    FROM contexts
    JOIN sections_contexts ON contexts.ID = sections_contexts.Context_ID
    JOIN article_sections ON sections_contexts.Section_ID = article_sections.Section_ID
    JOIN articles ON article_sections.Article_ID = articles.ID
    WHERE contexts.ID = ?
    """
  cursor.execute(query_article_id, (context_id,))
  article_id_result = cursor.fetchone()

  if article_id_result is None:
    conn.close()
    raise Exception(f"Context ID {context_id} not found in the database")

  article_id = article_id_result[0]
  outline = article_id_result[1]

  query_contexts = """
    SELECT contexts.Section_Num, contexts.num_tokens, contexts.Section_Title, contexts.text, contexts.ID
    FROM contexts
    JOIN sections_contexts ON contexts.ID = sections_contexts.Context_ID
    JOIN article_sections ON sections_contexts.Section_ID = article_sections.Section_ID
    WHERE article_sections.Article_ID = ?
    """
  cursor.execute(query_contexts, (article_id,))
  all_contexts = cursor.fetchall()

  query_current_context = """
    SELECT contexts.Section_Title, contexts.text
    FROM contexts
    WHERE contexts.ID = ?
    """
  cursor.execute(query_current_context, (context_id,))
  current_context = cursor.fetchone()

  conn.close()

  return all_contexts, current_context, outline


# Given a list of context data, I want to get the context id of the preivous context
def get_previous_context_id(context_data, current_context_id):
  for i in range(len(context_data)):
    if context_data[i][4] == current_context_id:
      if i == 0:
        return None
      else:
        return context_data[i - 1][4]


def get_next_context_id(context_data, current_context_id):
  for i in range(len(context_data)):
    if context_data[i][4] == current_context_id:
      if i == len(context_data) - 1:
        return None
      else:
        return context_data[i + 1][4]


# context_id = "DjkRDVj9YwkfmxAFfgXgJ"
# context_data, current_context, outline = get_context_given_contextID(context_id)
# for row in context_data:
#     print(f"Section Number: {row[0]}")
#     print(f"Number of Tokens: {row[1]}")
#     print(f"Section Title: {row[2]}")
#     print(f"ID: {row[4]}")
#     print("---")
# print(current_context)
# print(outline)
# print(get_previous_context_id(context_data, "DjkRDVj9YwkfmxAFfgXgJ"))
# print(get_next_context_id(context_data, "lcP1p88Pu0hIP-rgWzf2u"))
