from typing import Dict, Any, List, Optional
from sqlalchemy import text
from langchain_community.utilities import SQLDatabase

def create_db_connection(connection_string: str, connection_params: Optional[Dict[str, str]] = None) -> SQLDatabase:
    """Create a database connection using the provided connection string."""
    if connection_params is None:
        connection_params = {
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
            "timeout": "60",
            "connection_timeout": "60"
        }
    
    params = "&".join(f"{key}={value}" for key, value in connection_params.items())
    sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={connection_string}&{params}"
    
    return SQLDatabase.from_uri(sqlalchemy_url)

def execute_sql_query(db: SQLDatabase, query: str) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results."""
    try:
        with db._engine.connect() as connection:
            result = connection.execute(text(query))
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        print(f"Error executing SQL query: {str(e)}")
        return None

def extract_sql_query(response: str) -> Optional[str]:
    """Extract SQL query from response dictionary."""
    if isinstance(response, dict):
        return response.get('sql_query')
    return None