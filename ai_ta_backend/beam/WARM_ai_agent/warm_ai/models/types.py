from typing import List, Dict, Any, Optional, Literal, TypedDict

class SQLResponse(TypedDict):
    explanation: str
    sql_query: str
    suggested_actions: List[str]

class QueryIntent(TypedDict):
    needs_location: bool
    location_terms: List[str]
    query_type: Literal["location_specific", "general", "comparison"]

class AgentState(TypedDict):
    question: str
    intent: Optional[QueryIntent]
    processed_question: Optional[str]
    sql_response: Optional[SQLResponse]
    results: Optional[List[Dict[str, Any]]]
    error: Optional[str]