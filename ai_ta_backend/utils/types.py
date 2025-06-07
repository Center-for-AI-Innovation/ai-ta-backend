import datetime
from typing import Any, Dict, List, Optional

import pydantic


class DocumentMetadata(pydantic.BaseModel):
  authors: list[str]
  journal_name: str
  publication_date: datetime.date  # Changed from datetime.date to str
  keywords: list[str]
  doi: str
  title: str
  subtitle: Optional[str]
  visible_urls: list[str]
  field_of_science: str
  concise_summary: str
  specific_questions_document_can_answer: list[str]
  additional_fields: Optional[Dict[str, Any]] = {}

  # Can't get this to work properly
  # class Config:
  #     extra = pydantic.Extra.allow  # Allow arbitrary additional fields


class GrobidMetadata(pydantic.BaseModel):
  """
  additional_fields is for the paper "sections" with arbitrary section names. 
  Currently, the SQLite DB will have a separate column for every unique "major_sec_title". 
  We'll see how messy it gets... maybe LLMs can normalize this some.

  Format of additional_fields:
  {
    "major_sec_num": 3,
    "major_sec_title": "Extracting Metadata",
    "text": "In the previous section, we...", # full text of the section
    "tokens": 1067
  }
  """
  uuid: str
  filepath: str
  total_tokens: int
  avg_tokens_per_section: int
  max_tokens_per_section: int
  all_sections: Dict[str, str]
  additional_fields: Optional[List[Dict[str, Any]]] = [{}]


# Prisma data model https://prisma-client-py.readthedocs.io/en/stable/
# TBH I'd rather invest in learning real SQL. Theo switched from Drizzle to Prisma... no long-term security in either.
# model DocumentMetadata {
#   id                             Int      @id @default(autoincrement())
#   authors                        String[]
#   journalName                    String
#   publicationDate                DateTime
#   keywords                       String[]
#   doi                            String
#   title                          String
#   subtitle                       String?   // Optional field
#   visibleUrls                    String[]
#   fieldOfScience                 String
#   conciseSummary                 String
#   specificQuestionsDocumentCanAnswer String[]
# }
