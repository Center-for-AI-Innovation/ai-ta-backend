"""Data models for PubMed pipeline."""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class ArticleRow:
    """Parsed article data from PubMed XML."""
    pmid: int
    pmcid: Optional[str]
    doi: Optional[str]
    title: Optional[str]
    abstract: Optional[str]
    publication_date: Optional[date]
