# Standard library imports
from typing import Dict, Any, List, Literal

# Third-party imports
from sqlalchemy import text
from langgraph.graph import StateGraph, END
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from ..models.state_manager import StateManager

# Local imports
from ..models.types import AgentState, SQLResponse, QueryIntent
from ..models.prompts import PromptTemplates
from ..utils.location import LocationMapper
from ..utils.sql import extract_sql_query

class SQLAIAgent:
    """
    AI agent for handling SQL queries with natural language processing capabilities.
    Includes location mapping and query intent classification.
    """

    def __init__(self, connection_string: str, openai_api_key: str):
        self.connection_string = connection_string
        self.openai_api_key = openai_api_key
        self.db = None
        
        # Initialize prompts
        self.base_system_prompt = PromptTemplates.create_base_system_prompt()
        self.intent_prompt = PromptTemplates.create_intent_prompt()
        self.intent_parser = JsonOutputParser()
        self.query_parser = JsonOutputParser()
        self.intent_chain = None
        self.query_chain = None
        
        # Create workflow
        self.workflow = self._create_graph()

    def _classify_intent(self, state: AgentState) -> AgentState:
        """Classify the intent of the user's question."""
        try:
            intent = self.intent_chain.invoke(state["question"])
            if not intent:
                return {
                    "error": "Intent classification returned empty result",
                    **state,
                    "intent": {
                        "needs_location": False,
                        "location_terms": [],
                        "query_type": "general"
                    }
                }
            return {"intent": intent, **state}
        except Exception as e:
            # Provide a default intent when classification fails
            default_intent = {
                "needs_location": False,
                "location_terms": [],
                "query_type": "general"
            }
            return {
                "error": f"Intent classification failed: {str(e)}",
                **state,
                "intent": default_intent
            }
    
    def _execute_sql(self, state: AgentState) -> AgentState:
        """Execute the generated SQL query."""
        if not state.get("sql_response"):
            return {"error": "No SQL query to execute", **state}
        
        try:
            results = self.execute_sql(state["sql_response"]["sql_query"])
            return {"results": results, **state}
        except Exception as e:
            return {"error": f"SQL execution failed: {str(e)}", **state}
    
    def _process_locations_helper(self, state: Dict[str, Any]) -> str:
        """Helper function to process location terms in the question."""
        question = state["question"]
        intent = state["intent"]
        
        for location in intent["location_terms"]:
            station_code = LocationMapper.get_station_code(location)
            if station_code:
                question = question.lower().replace(
                    location.lower(),
                    f"{location} (Station: {station_code})"
                )
        
        return question
    
    def _process_locations(self, state: AgentState) -> AgentState:
        """Process any location references in the question."""
        if not state.get("intent"):
            return {**state, "error": "No intent found in state"}
        
        if not state["intent"].get("needs_location", False):
            return {**state, "processed_question": state["question"]}
        
        try:
            processed = self._process_locations_helper(state)
            return {**state, "processed_question": processed}
        except Exception as e:
            return {**state, "error": f"Location processing failed: {str(e)}"}

    def _generate_sql(self, state: AgentState) -> AgentState:
        """Generate SQL query from processed question."""
        if state.get("error"):
            return state
        
        try:
            question = state.get("processed_question", state["question"])
            # Update how we invoke the query chain
            sql_response = self.query_chain.invoke({
                "question": question,
                "schema": self.db.get_table_info(),
                "location_mappings": LocationMapper.get_all_mappings()
            })
            return {**state, "sql_response": sql_response}
        except Exception as e:
            return {**state, "error": f"SQL generation failed: {str(e)}"}

    def _should_execute_sql(self, state: AgentState) -> Literal["execute", "error", "end"]:
        """Determine if SQL should be executed based on current state."""
        if state.get("error"):
            return "error"
        if state.get("sql_response"):
            return "execute"
        return "end"
    
    def _create_graph(self) -> StateGraph:
        """Create the workflow graph for processing queries."""
        workflow = StateGraph(AgentState)

        workflow.add_node("classify_intent", self._classify_intent)
        workflow.add_node("process_locations", self._process_locations)
        workflow.add_node("generate_sql", self._generate_sql)
        workflow.add_node("execute_sql", self._execute_sql)

        workflow.add_edge("classify_intent", "process_locations")
        workflow.add_edge("process_locations", "generate_sql")
        
        workflow.add_conditional_edges(
            "generate_sql",
            self._should_execute_sql,
            {
                "execute": "execute_sql",
                "error": END,
                "end": END
            }
        )
        
        workflow.add_edge("execute_sql", END)
        workflow.set_entry_point("classify_intent")

        return workflow.compile()

    def connect(self):
        """Establish database connection and initialize LLM components."""
        try:
            connection_params = {
                "driver": "{ODBC Driver 18 for SQL Server}",
                "TrustServerCertificate": "yes",
                "Encrypt": "yes",  # Added for Ubuntu
                "timeout": "60",
                "connection_timeout": "60"
            }
            
            params = "&".join(f"{key}={value}" for key, value in connection_params.items())
            sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={self.connection_string}&{params}"
            
            self.db = SQLDatabase.from_uri(sqlalchemy_url)
            
            llm = ChatOpenAI(temperature=0, api_key=self.openai_api_key, model="gpt-4o-mini")

            # Update the query prompt to include all required variables
            self.query_prompt = ChatPromptTemplate.from_messages([
                ("system", self.base_system_prompt + """
                Schema: {schema}
                Location Mappings: {location_mappings}
                
                You must respond in the following JSON format:
                {
                    "explanation": "A plain English explanation of what the query does",
                    "sql_query": "The SQL query to execute",
                    "suggested_actions": ["Optional list of follow-up actions or warnings"]
                }
                """),
                ("human", "{question}")
            ])

            self.query_parser = JsonOutputParser(pydantic_object=SQLResponse)
            self.intent_parser = JsonOutputParser(pydantic_object=QueryIntent)
            
            self.intent_chain = (
                self.intent_prompt 
                | llm 
                | self.intent_parser
            )
            
            self.query_chain = (
                self.query_prompt 
                | llm 
                | self.query_parser
            )

            print("Intent and query chains initialized successfully")
            # Verify initialization
            StateManager.verify_initialization(
                self,
                ["intent_chain", "query_chain"]
            )
            
            location_chain = (
                self.query_prompt 
                | llm 
                | self.query_parser
            )

            direct_chain = (
                self.query_prompt 
                | llm 
                | self.query_parser
            )

            def branch_chain(inputs: dict):
                question = inputs["question"]
                intent_result = self.intent_chain.invoke(question)
                
                if intent_result["needs_location"]:
                    processed_question = self._process_locations({
                        "question": question, 
                        "intent": intent_result
                    })
                    return location_chain.invoke(processed_question)
                else:
                    return direct_chain.invoke(question)

            self.chain = branch_chain

            toolkit = SQLDatabaseToolkit(db=self.db, llm=llm)
            self.agent_executor = create_sql_agent(
                llm=llm,
                toolkit=toolkit,
                verbose=True,
                agent_type="tool-calling"
            )

            print("Successfully connected to the database")
            
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            raise

    def execute_sql(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            with self.db._engine.connect() as connection:
                result = connection.execute(text(query))
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            print(f"Error executing SQL query: {str(e)}")
            return None

    def _verify_initialization(self):
        """Verify that all required attributes are initialized."""
        required_attributes = [
            'intent_prompt',
            'intent_chain',
            'intent_parser',
            'query_prompt',
            'query_parser',
            'db',
            'workflow'
        ]
        
        missing = []
        for attr in required_attributes:
            if not hasattr(self, attr):
                missing.append(attr)
            elif getattr(self, attr) is None:
                missing.append(f"{attr} (None)")
        
        if missing:
            raise RuntimeError(f"Missing or None attributes: {', '.join(missing)}")
    
    def query(self, question: str) -> Dict[str, Any]:
        """Process a natural language query and return results."""
        try:
            self._verify_initialization()

            initial_state = AgentState(
                question=question,
                intent=None,
                processed_question=None,
                sql_response=None,
                results=None,
                error=None
            )
            
            final_state = self.workflow.invoke(initial_state)
            
            if final_state.get("error"):
                print(f"Error in workflow: {final_state['error']}")
                agent_response = self.agent_executor.invoke({"input": question})
                return {
                    "explanation": "Fallback to agent executor",
                    "sql_query": extract_sql_query(agent_response),
                    "results": agent_response,
                    "suggested_actions": ["Workflow failed, used fallback agent"]
                }
            
            return {
                "explanation": final_state.get("sql_response", {}).get("explanation"),
                "sql_query": final_state.get("sql_response", {}).get("sql_query"),
                "results": final_state.get("results"),
                "suggested_actions": final_state.get("sql_response", {}).get("suggested_actions", [])
            }
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return {
                "error": str(e),
                "results": None,
                "sql_query": None,
                "suggested_actions": ["Error occurred, please try again"]
            }
        
    def disconnect(self):
        """Close database connection."""
        if hasattr(self, 'db') and self.db:
            if hasattr(self.db, '_engine'):
                self.db._engine.dispose()