import os
from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_openai import ChatOpenAI

class GraphDatabase:
    def __init__(self):
        self.clinical_kg_graph = Neo4jGraph(
            url=os.environ['CKG_NEO4J_URI'],
            username=os.environ['CKG_NEO4J_USERNAME'],
            password=os.environ['CKG_NEO4J_PASSWORD'],
            database=os.environ['CKG_NEO4J_DATABASE'],
            refresh_schema=True,
        )

        self.prime_kg_graph = Neo4jGraph(
            url=os.environ['PRIME_KG_NEO4J_URI'],
            username=os.environ['PRIME_KG_NEO4J_USERNAME'],
            password=os.environ['PRIME_KG_NEO4J_PASSWORD'],
            database=os.environ['PRIME_KG_NEO4J_DATABASE'],
            refresh_schema=True,
        )
        
        # Get schema information for the system prompt
        self.ckg_schema_info = self._get_schema_info(self.clinical_kg_graph)
        self.prime_kg_schema_info = self._get_schema_info(self.prime_kg_graph)
        
        # Create the chain with the clinical KG system prompt
        self.ckg_chain = self._create_clinical_kg_chain()
        self.prime_kg_chain = self._create_prime_kg_chain()
    
    def _get_schema_info(self, graph):
        """Extract schema information from the Neo4j database."""
        try:    
            # This will get the schema without refreshing it (faster)
            return graph.schema
        except:
            # If schema isn't available yet, return a placeholder
            return "Schema information not available. Please refresh schema first."
    
    def _create_clinical_kg_chain(self):
        """Create a GraphCypherQAChain with a clinical KG system prompt."""
        
        schema_info = self.ckg_schema_info
        system_prompt = f"""
        You are a clinical knowledge graph expert assistant that helps healthcare professionals query a medical knowledge graph.
        
        SCHEMA INFORMATION:
        {schema_info}
        
        GUIDELINES FOR GENERATING CYPHER QUERIES:
            1. Always use the correct node labels and relationship types from the schema information. Pay attention to the formatting of the node and relationship names.
            2. Identify the key entities from the user query and use the most specific node type available. Analyze and choose the relationships in the schema carefully.
            3. Use appropriate WHERE clauses with case-insensitive matching:
            - For exact matches: WHERE toLower(n.name) = toLower("term")
            - For partial matches: WHERE toLower(n.name) CONTAINS toLower("term")
            4. For complex queries, use multiple MATCH clauses rather than long path patterns.
            5. Include LIMIT clauses (typically 5-15 results) for readability.
            6. For property access, use the correct property names from the schema.
            7. When appropriate, use aggregation functions (count, collect, etc.).
            8. For path finding, consider using shortest path algorithms.
            9. Return the most clinically relevant properties in the RETURN clause.
            10. When generating the Cypher query, read through the schema information and try out different combinations of node labels and relationship types to find the most relevant ones.
            
        RESPONSE FORMAT:
            1. First, explain the Cypher query you're generating and why it addresses the user's question.
            2. If the response from neo4j is empty, return "No results found".
            3. If the response from neo4j is not empty, return the same in a list of dictionaries along with the full context.
        """
        print("SYSTEM PROMPT: ", system_prompt)
        return GraphCypherQAChain.from_llm(
            ChatOpenAI(temperature=0, model="gpt-4o"),
            graph=self.clinical_kg_graph,
            verbose=False,
            allow_dangerous_requests=True,
            system_message=system_prompt,
        )
    
    def create_chain_with_custom_prompt(self, additional_instructions=""):
        """
        Create a new chain with a custom prompt that includes additional instructions.
        
        Args:
            additional_instructions (str): Additional instructions to add to the system prompt
            
        Returns:
            GraphCypherQAChain: A new chain with the custom prompt
        """
        system_prompt = f"""
        You are a clinical knowledge graph expert assistant that helps healthcare professionals query a medical knowledge graph.
        
        SCHEMA INFORMATION:
        {self.ckg_schema_info}
        
        GUIDELINES FOR GENERATING CYPHER QUERIES:
        1. Always use the correct node labels and relationship types from the schema above
        2. For clinical entities, prefer to use specific node types like Disease, Drug, Symptom, etc.
        3. When searching for treatments, use relationships like TREATS, PRESCRIBED_FOR, etc.
        4. For finding side effects, use relationships like CAUSES, HAS_SIDE_EFFECT, etc.
        5. When querying for interactions, look for INTERACTS_WITH relationships
        6. Limit results to a reasonable number (e.g., LIMIT 10) for readability
        7. Include relevant properties in the RETURN clause
        8. Use appropriate WHERE clauses to filter results
        9. For text matching, use case-insensitive matching with toLower() or CONTAINS
        10. For complex queries, consider using multiple MATCH clauses
        
        RESPONSE FORMAT:
        1. First, explain the Cypher query you're generating and why
        2. Present the results in a clear, structured format
        3. Provide a clinical interpretation of the results
        4. If relevant, suggest follow-up queries the user might be interested in
        
        Remember that you're helping healthcare professionals, so be precise and clinically accurate.
        
        ADDITIONAL INSTRUCTIONS:
        {additional_instructions}
        """
        print("SYSTEM PROMPT: ", system_prompt)
        
        return GraphCypherQAChain.from_llm(
            ChatOpenAI(temperature=0, model="gpt-4o"),
            graph=self.clinical_kg_graph,
            verbose=False,
            allow_dangerous_requests=True,
            system_message=system_prompt,
        )
    
    def refresh_schema(self, graph):
        """Refresh the schema and update the chain with the new schema information."""
        graph.refresh_schema()
        schema_info = self._get_schema_info(graph)
        chain = self._create_clinical_kg_chain()
        return "Schema refreshed successfully"
    
    def _create_prime_kg_chain(self):
        """Create a GraphCypherQAChain with a prime KG system prompt."""
        schema_info = self.prime_kg_schema_info
        system_prompt = f"""
        You are a prime knowledge graph expert assistant that helps healthcare professionals query a medical knowledge graph.
        
        SCHEMA INFORMATION:
        {schema_info}
        
        GUIDELINES FOR GENERATING CYPHER QUERIES:
        1. Always use the correct node labels and relationship types from the schema above
        2. For clinical entities, prefer to use specific node types like Disease, Drug, Symptom, etc.
        3. When searching for treatments, use relationships like TREATS, PRESCRIBED_FOR, etc.
        4. For finding side effects, use relationships like CAUSES, HAS_SIDE_EFFECT, etc.
        5. When querying for interactions, look for INTERACTS_WITH relationships
        6. Limit results to a reasonable number (e.g., LIMIT 10) for readability
        """
        
        return GraphCypherQAChain.from_llm(
            ChatOpenAI(temperature=0, model="gpt-4o"),
            graph=self.prime_kg_graph,
            verbose=False,
            allow_dangerous_requests=True,
            system_message=system_prompt,
        )
        
