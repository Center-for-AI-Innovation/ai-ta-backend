from typing import Dict, Any, List, Literal
from ..models.types import AgentState, QueryIntent

class StateManager:
    """Manages state transitions and validations for the SQL AI Agent."""
    
    @staticmethod
    def create_initial_state(question: str) -> AgentState:
        """Create initial state for a new query."""
        return AgentState(
            question=question,
            intent=None,
            processed_question=None,
            sql_response=None,
            results=None,
            error=None
        )

    @staticmethod
    def process_locations_helper(state: Dict[str, Any], question: str, intent: QueryIntent) -> str:
        """Helper function to process location terms in the question."""
        from ..utils.location import LocationMapper  # Import here to avoid circular imports
        
        for location in intent["location_terms"]:
            station_code = LocationMapper.get_station_code(location)
            if station_code:
                question = question.lower().replace(
                    location.lower(),
                    f"{location} (Station: {station_code})"
                )
        return question

    @staticmethod
    def should_execute_sql(state: AgentState) -> Literal["execute", "error", "end"]:
        """Determine if SQL should be executed based on current state."""
        if state.get("error"):
            return "error"
        if state.get("sql_response"):
            return "execute"
        return "end"

    @staticmethod
    def verify_initialization(instance: Any, required_attributes: List[str]) -> None:
        """Verify that all required attributes are initialized."""
        missing = []
        for attr in required_attributes:
            if not hasattr(instance, attr):
                missing.append(attr)
            elif getattr(instance, attr) is None:
                missing.append(f"{attr} (None)")
        
        if missing:
            raise RuntimeError(f"Missing or None attributes: {', '.join(missing)}")