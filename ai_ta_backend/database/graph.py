import os
import re

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict
from flask import current_app


# Unified state for both PrimeKG and Clinical KG
class KGQueryState(TypedDict):
    user_query: str
    attempt: int
    queries_tried: list[str]
    results: list[dict] | dict  # allow both for compatibility
    max_attempts: int

# Example strategies for generating Cypher queries (can be extended)
def generate_primekg_cypher(user_query: str, attempt: int) -> str:
    """
    Generate different Cypher queries for each attempt, aligned with the system prompt instructions.
    For queries mentioning two entities, retries after the first attempt will use CONTAINS for node names and match any relationship type between the nodes.
    """
    general_terms = ["related to", "associated with", "connected to", "linked to", "connection", "relationship"]
    lower_query = user_query.lower()
    uses_general_term = any(term in lower_query for term in general_terms)

    # Simple heuristic: look for two quoted entities or two 'and'-separated terms
    # e.g., "diabetes and heart disease"
    entity_match = re.findall(r'([\w\- ]+) and ([\w\- ]+)', user_query, re.IGNORECASE)
    if entity_match:
        entity1, entity2 = entity_match[0]
        entity1 = entity1.strip(' "')
        entity2 = entity2.strip(' "')
    else:
        entity1 = entity2 = None

    if attempt == 0:
        # First attempt: smart mapping and synonym use for node labels/relationships
        return f"{user_query} (map user terms to closest schema node labels/relationships, use synonyms if needed)"
    elif (attempt == 1 or attempt == 2) and entity1 and entity2:
        # For retries, if two entities are detected, use CONTAINS and match any relationship type
        return (
            f"Find any connections between entities using partial matching and any relationship type: "
            f'MATCH (n1), (n2) '
            f'WHERE toLower(n1.node_name) CONTAINS "{entity1.lower()}" '
            f'AND toLower(n2.node_name) CONTAINS "{entity2.lower()}" '
            f'MATCH (n1)-[r]-(n2) '
            f'RETURN n1.node_name AS Entity1, n2.node_name AS Entity2, type(r) AS RelationshipType, r'
        )
    elif attempt == 1 and uses_general_term:
        # Second attempt: broaden to any plausible relationship if general terms are detected
        return f"{user_query} (broaden: treat general terms like 'related to' as any plausible relationship, use -[]-> or multiple types)"
    elif attempt == 2:
        # Third attempt: try alternative node labels/relationships and synonyms
        return f"{user_query} (try alternative node labels, relationship types, and synonyms from schema)"
    else:
        # Fallback: most general query
        return f"{user_query} (fallback: use the most general relationship and node label patterns)"

# Example strategies for generating Cypher queries for Clinical KG (can be extended)
def generate_clinicalkg_cypher(user_query: str, attempt: int) -> str:
    """
    Generate different Cypher queries for each attempt, aligned with the clinical KG system prompt instructions.
    """
    general_terms = ["related to", "associated with", "connected to", "linked to", "connection", "relationship"]
    lower_query = user_query.lower()
    uses_general_term = any(term in lower_query for term in general_terms)

    # Simple heuristic: look for two quoted entities or two 'and'-separated terms
    entity_match = re.findall(r'([\w\- ]+) and ([\w\- ]+)', user_query, re.IGNORECASE)
    if entity_match:
        entity1, entity2 = entity_match[0]
        entity1 = entity1.strip(' "')
        entity2 = entity2.strip(' "')
    else:
        entity1 = entity2 = None

    if attempt == 0:
        # First attempt: smart mapping and synonym use for node labels/relationships
        return f"{user_query} (map user terms to closest schema node labels/relationships, use synonyms if needed)"
    elif (attempt == 1 or attempt == 2) and entity1 and entity2:
        # For retries, if two entities are detected, use CONTAINS and match any relationship type
        return (
            f"Find any connections between entities using partial matching and any relationship type: "
            f'MATCH (n1), (n2) '
            f'WHERE toLower(n1.name) CONTAINS "{entity1.lower()}" '
            f'AND toLower(n2.name) CONTAINS "{entity2.lower()}" '
            f'MATCH (n1)-[r]-(n2) '
            f'RETURN n1.name AS Entity1, n2.name AS Entity2, type(r) AS RelationshipType, r'
        )
    elif attempt == 1 and uses_general_term:
        # Second attempt: broaden to any plausible relationship if general terms are detected
        return f"{user_query} (broaden: treat general terms like 'related to' as any plausible relationship, use -[]-> or multiple types)"
    elif attempt == 2:
        # Third attempt: try alternative node labels/relationships and synonyms
        return f"{user_query} (try alternative node labels, relationship types, and synonyms from schema)"
    else:
        # Fallback: most general query
        return f"{user_query} (fallback: use the most general relationship and node label patterns)"

def run_primekg_chain(chain, cypher_query: str):
    # This function should call the chain with the cypher_query
    # For now, assume chain.invoke returns a dict with 'results' key
    # In practice, you may need to adapt this to your chain's API
    try:
        result = chain.invoke({"query": cypher_query})
        # Adapt this if your chain returns results differently
        if isinstance(result, dict) and "results" in result:
            return result["results"]
        return result
    except Exception as e:
        return []

class GraphDatabase:

  def __init__(self):
    # Load environment variables once
    self.ckg_neo4j_uri = os.environ['CKG_NEO4J_URI']
    self.ckg_neo4j_username = os.environ['CKG_NEO4J_USERNAME']
    self.ckg_neo4j_password = os.environ['CKG_NEO4J_PASSWORD']
    self.ckg_neo4j_database = os.environ['CKG_NEO4J_DATABASE']
    self.prime_kg_neo4j_uri = os.environ['PRIME_KG_NEO4J_URI']
    self.prime_kg_neo4j_username = os.environ['PRIME_KG_NEO4J_USERNAME']
    self.prime_kg_neo4j_password = os.environ['PRIME_KG_NEO4J_PASSWORD']
    self.prime_kg_neo4j_database = os.environ['PRIME_KG_NEO4J_DATABASE']
    self.vlads_openai_key = os.environ['VLADS_OPENAI_KEY']

    self.clinical_kg_graph = Neo4jGraph(
        url=self.ckg_neo4j_uri,
        username=self.ckg_neo4j_username,
        password=self.ckg_neo4j_password,
        database=self.ckg_neo4j_database,
        refresh_schema=True,
    )

    try:
      count = self.clinical_kg_graph.query("MATCH (n) RETURN count(n) AS node_count LIMIT 1")
      # print(f"[DEBUG][GraphDatabase] Connected to Clinical KG Neo4j. Node count: {count[0]['node_count']}")
    except Exception as e:
      # print(f"[ERROR][GraphDatabase] Could not connect to Clinical KG Neo4j: {e}")
      pass

    self.prime_kg_graph = Neo4jGraph(
        url=self.prime_kg_neo4j_uri,
        username=self.prime_kg_neo4j_username,
        password=self.prime_kg_neo4j_password,
        database=self.prime_kg_neo4j_database,
        refresh_schema=True,
    )

    try:
      count = self.prime_kg_graph.query("MATCH (n) RETURN count(n) AS node_count LIMIT 1")
      # print(f"[DEBUG][GraphDatabase] Connected to Prime KG Neo4j. Node count: {count[0]['node_count']}")
    except Exception as e:
      # print(f"[ERROR][GraphDatabase] Could not connect to Prime KG Neo4j: {e}")
      pass

    # Get schema information for the system prompt
    self.ckg_schema_info = self._get_schema_info(self.clinical_kg_graph)
    # print(f"[DEBUG][GraphDatabase] Clinical KG schema info loaded. Type: {type(self.ckg_schema_info)}, Length: {len(str(self.ckg_schema_info))}")
    self.prime_kg_schema_info = self._get_schema_info(self.prime_kg_graph)
    # print(f"[DEBUG][GraphDatabase] Prime KG schema info loaded. Type: {type(self.prime_kg_schema_info)}, Length: {len(str(self.prime_kg_schema_info))}")

    # Create the chain with the clinical KG system prompt
    try:
      self.ckg_chain = self._create_clinical_kg_chain()
      # print("[DEBUG][GraphDatabase] Clinical KG chain created successfully.")
    except Exception as e:
      # print(f"[ERROR][GraphDatabase] Failed to create Clinical KG chain: {e}")
      import traceback
      traceback.print_exc()
      self.ckg_chain = None
    try:
      self.prime_kg_chain = self._create_prime_kg_chain()
      # print("[DEBUG][GraphDatabase] Prime KG chain created successfully.")
    except Exception as e:
      # print(f"[ERROR][GraphDatabase] Failed to create Prime KG chain: {e}")
      import traceback
      traceback.print_exc()
      self.prime_kg_chain = None

  def refresh_schema(self, graph):
    """Refresh the schema and update the chain with the new schema information."""
    graph.refresh_schema()

    return "Schema refreshed successfully"

  def _get_schema_info(self, graph):
    """Extract schema information from the Neo4j database."""
    try:
      # This will get the schema without refreshing it (faster)
      return graph.schema
    except:
      # If schema isn't available yet, return a placeholder
      return "Schema information not available. Please refresh schema first."

  def _create_chain(self, schema_info, system_prompt, graph, return_direct=True, return_intermediate_steps=False, verbose=True):
    """
    Generic chain creation helper for GraphCypherQAChain.
    """
    return GraphCypherQAChain.from_llm(
        ChatOpenAI(temperature=0, model="gpt-4o", api_key=self.vlads_openai_key),
        graph=graph,
        return_direct=return_direct,
        return_intermediate_steps=return_intermediate_steps,
        verbose=verbose,
        allow_dangerous_requests=True,
        system_message=system_prompt,
    )

  def _create_clinical_kg_chain(self):
    """Create a GraphCypherQAChain with a clinical KG system prompt."""
    schema_info = self.ckg_schema_info
    system_prompt = f"""
      You are a clinical knowledge graph expert assistant that helps healthcare professionals query a medical knowledge graph.
      
      SCHEMA INFORMATION:
      {schema_info}
      
      GUIDELINES FOR GENERATING CYPHER QUERIES:
      1. Always use the correct node labels and relationship types from the schema information.
      2. Identify key entities from the user query and use the most specific node type available.
      3. Use appropriate WHERE clauses with case-insensitive matching:
        - For exact matches: WHERE toLower(n.name) = toLower("term")
        - For partial matches: WHERE toLower(n.name) CONTAINS toLower("term")
      4. For complex queries, use multiple MATCH clauses rather than long path patterns.
      5. Include LIMIT clauses (typically 5-15 results) for readability.
      6. Use correct property names from the schema.
      7. Use aggregation functions (count, collect, etc.) when appropriate.
      8. For path finding, consider using shortest path algorithms.
      9. Return the most clinically relevant properties in the RETURN clause.
      10. Try different combinations of node labels and relationship types to find the most relevant ones.
      
      RESPONSE FORMAT:
      1. First, explain the Cypher query you're generating and why it addresses the user's question.
      2. Present the Cypher query.
      3. If the response is empty, return "No results found" and try alternative queries.
      4. If results are found, present them as a list of dictionaries with relevant properties.
      
      EXAMPLES:

      Example 1: Protein-Cellular Component Association
      User query: "Which cellular components is the protein EGR1 associated with?"

      Explain: To answer this question, I'll search for the Protein node with the name 'EGR1' and find all 
      Cellular_component nodes connected to it via the ASSOCIATED_WITH relationship. I'll also return the 
      evidence type and source for each association.

      Cypher:
      MATCH (p:Protein)-[r:ASSOCIATED_WITH]->(cc:Cellular_component)
      WHERE toLower(p.name) = "egr1"
      RETURN 
          p.name AS Protein, 
          cc.name AS CellularComponent, 
          cc.id AS CellularComponentID, 
          r.evidence_type AS EvidenceType, 
          r.source AS Source
      ORDER BY cc.name

      Example 2: Disease Pathology Samples
      User query: "List proteins detected in pathology samples for pancreatic cancer."

      Explain: I'll search for Disease nodes with the name 'pancreatic cancer' and find all Protein nodes 
      connected via the DETECTED_IN_PATHOLOGY_SAMPLE relationship, including expression levels and prognosis data.

      Cypher:
      MATCH (p:Protein)-[r:DETECTED_IN_PATHOLOGY_SAMPLE]->(d:Disease)
      WHERE toLower(d.name) = "pancreatic cancer"
      RETURN 
          p.name AS Protein, 
          d.name AS Disease, 
          r.expression_low AS ExpressionLow, 
          r.expression_medium AS ExpressionMedium, 
          r.expression_high AS ExpressionHigh, 
          r.not_detected AS NotDetected, 
          r.positive_prognosis_logrank_pvalue AS PositivePrognosisP, 
          r.negative_prognosis_logrank_pvalue AS NegativePrognosisP, 
          r.linkout AS Link
      ORDER BY p.name

      Example 3: Gene Variants
      User query: "List all genes that have a known missense variant."

      Explain: I'll search for Known_variant nodes with the effect 'missense variant' and find all Gene nodes 
      connected via the VARIANT_FOUND_IN_GENE relationship, returning gene and variant information.

      Cypher:
      MATCH (v:Known_variant)-[:VARIANT_FOUND_IN_GENE]->(g:Gene)
      WHERE toLower(v.effect) = "missense variant"
      RETURN 
          g.name AS Gene, 
          v.pvariant_id AS Variant, 
          v.external_id AS ExternalID
      ORDER BY g.name, v.pvariant_id
      LIMIT 15

      Note: For each query, if no results are found, try alternative approaches such as:
      - Using different relationship types
      - Broadening search terms
      - Checking for synonyms
      - Using different node properties
      Always explain the alternative approaches being tried.
      """
    if current_app and current_app.debug:
        print("SYSTEM PROMPT: ", system_prompt)
    return self._create_chain(schema_info, system_prompt, self.clinical_kg_graph)

  def _create_prime_kg_chain(self):
    """Create a GraphCypherQAChain with a prime KG system prompt."""
    schema_info = self.prime_kg_schema_info
    system_prompt = f"""
    You are a clinical knowledge graph expert assistant that helps healthcare professionals query a medical knowledge graph.
    
    SCHEMA INFORMATION:
    {schema_info}
    
    GUIDELINES FOR GENERATING CYPHER QUERIES:
    1. Always use the correct node labels (e.g., `gene_protein`, Disease, Drug) and relationship types (e.g., "protein_protein", "disease_gene") as per the schema.
    2. Use node properties node_name and node_id for matching entities. Prefer case-insensitive matching for node_name (e.g., toLower(n.node_name) CONTAINS toLower("...")) for partial matches.
    3. For relationships, use the type (e.g., disease_gene for disease-gene associations) and, if relevant, filter on display_relation.
    4. For clinical/biomedical queries, prefer specific node types (e.g., Disease, Drug, gene_protein, Phenotype).
    5. When a user query mentions a disease (e.g., "cancer"), match Disease nodes where node_name contains the disease term (case-insensitive).
    6. To find related genes, look for relationships between Disease nodes and gene_protein nodes (e.g., disease_gene).
    7. Limit results to a reasonable number (e.g., LIMIT 10) for readability.
    8. For complex queries, use multiple MATCH clauses rather than long path patterns.
    9. Always return the most relevant properties (e.g., node_name, node_id, display_relation) in the RETURN clause.
    10. For ambiguous queries, try multiple plausible node labels or relationship types, and explain your reasoning.
    11. If no results are found, try up to 3 alternative queries with different node labels or relationship types.

    ADDITIONAL INSTRUCTIONS:
    - When translating user queries, always try to map user-provided terms (for node labels and relationship types) to the closest matching schema terms. If a direct match is not found, use a synonym or the most relevant node label or relationship type from the schema.
    - If the user uses general terms like "related to", "associated with", or "connected to", interpret these as any plausible relationship type, not just a specific relationship. Use a broad relationship pattern (e.g., -[]->) or try multiple plausible relationship types.
    - Be proactive in using synonyms and schema knowledge to maximize the chance of retrieving relevant results, even on the first attempt.

    RESPONSE FORMAT:
    1. First, explain the Cypher query you are generating and why it addresses the user's question.
    2. Present the Cypher query.
    3. If the response from Neo4j is empty, return "No results found" and try a new query (up to 3 attempts).
    4. If results are found, present them as a list of dictionaries with relevant properties and provide a brief interpretation.

    EXAMPLE 1:
    User query: "What drugs are used to treat Alzheimer's disease?"

    Your response:
    - Explain: "To answer this question, I'll search for Disease nodes with 'Alzheimer' in their name and find Drug nodes connected to them via an indication relationship, which shows approved uses for drugs."
    - Cypher:
      MATCH (d:disease)<-[:indication]-(drug:drug)
      WHERE toLower(d.node_name) CONTAINS "alzheimer"
      RETURN DISTINCT d.node_name AS Disease, drug.node_name AS Drug
      ORDER BY d.node_name, drug.node_name

    EXAMPLE 2:
    User query: "What biological processes are associated with the BRCA1 gene?"

    Your response:
    - Explain: "I'll find the gene_protein node for BRCA1 and identify all biological processes connected to it through the bioprocess_protein relationship."
    - Cypher:
      MATCH (g:`gene_protein`)-[:bioprocess_protein]->(bp:biological_process)
      WHERE toLower(g.node_name) = "brca1"
      RETURN DISTINCT g.node_name AS Gene, bp.node_name AS BiologicalProcess
      ORDER BY bp.node_name

    EXAMPLE 3:
    User query: "What are the side effects of metformin?"

    Your response:
    - Explain: "To find side effects of metformin, I'll search for the drug node representing metformin and identify all effect_phenotype nodes connected to it via a drug_effect relationship."
    - Cypher:
      MATCH (d:drug)-[:drug_effect]->(e:`effect_phenotype`)
      WHERE toLower(d.node_name) = "metformin"
      RETURN DISTINCT d.node_name AS Drug, e.node_name AS SideEffect
      ORDER BY e.node_name

    EXAMPLE 4:
    User query: "Which genes are expressed in the heart?"

    Your response:
    - Explain: "I'll search for anatomy nodes related to 'heart' and find gene_protein nodes that are connected to these anatomy nodes via an anatomy_protein_present relationship, indicating genes expressed in this tissue."
    - Cypher:
      MATCH (a:anatomy)<-[:anatomy_protein_present]-(g:`gene_protein`)
      WHERE toLower(a.node_name) CONTAINS "heart"
      RETURN DISTINCT a.node_name AS Anatomy, g.node_name AS Gene
      ORDER BY a.node_name, g.node_name
      
    EXAMPLE 5:
    User query: "Which pathways involve the TNF gene?"

    Your response:
    - Explain: "I'll find the gene_protein node for TNF and identify all pathway nodes connected to it through the pathway_protein relationship."
    - Cypher:
      MATCH (g:`gene_protein`)-[:pathway_protein]->(p:pathway)
      WHERE toLower(g.node_name) = "tnf" OR toLower(g.node_name) = "tumor necrosis factor"
      RETURN DISTINCT g.node_name AS Gene, p.node_name AS Pathway
      ORDER BY p.node_name

    If no results, try alternative node labels or relationship types, and explain your reasoning.
  
    EXAMPLE 6:
    User query: "What proteins interact with the ACE2 receptor?"

    Your response:
    - Explain: "To find proteins that interact with ACE2, I'll search for the gene_protein node representing ACE2 and identify all other gene_protein nodes connected to it via a protein_protein relationship."
    - Cypher:
      MATCH (g1:`gene_protein`)-[:protein_protein]->(g2:`gene_protein`)
      WHERE toLower(g1.node_name) = "ace2"
      RETURN DISTINCT g1.node_name AS Protein, g2.node_name AS InteractingProtein
      ORDER BY g2.node_name
      
    EXAMPLE 7:
    User query: "What cellular components are associated with mitochondrial diseases?"

    Your response:
    - Explain: "I'll identify disease nodes related to mitochondria, find associated genes, and then discover the cellular components linked to those genes."
    - Cypher:
      MATCH (d:disease)-[:disease_protein]->(g:`gene_protein`)-[:cellcomp_protein]->(cc:cellular_component)
      WHERE toLower(d.node_name) CONTAINS "mitochondri"
      RETURN DISTINCT d.node_name AS Disease, g.node_name AS Gene, cc.node_name AS CellularComponent
      ORDER BY d.node_name, cc.node_name
    
    EXAMPLE 8:
    User query: "What genes are associated with congenital hyperinsulinism?"

    Your response:
    - Explain: "To answer this question, I'll search for Disease nodes related to hyperinsulinism and identify the gene_protein nodes connected to them through disease_protein relationships, which indicate genes associated with this condition."
    - Cypher:
      MATCH (d:disease)-[:disease_protein]->(g:`gene_protein`)
      WHERE toLower(d.node_name) CONTAINS "hyperinsulin"
      RETURN DISTINCT d.node_name AS Disease, g.node_name AS Gene
      ORDER BY d.node_name, g.node_name

    EXAMPLE 9:
    User query: "Which drugs interact with the TNF inhibitor adalimumab?"

    Your response:
    - Explain: "To find drugs that interact with adalimumab (a TNF inhibitor), I'll search for the drug node representing adalimumab and identify other drug nodes connected to it through drug_drug relationships, which indicate potential drug interactions."
    - Cypher:
      MATCH (d1:drug)-[r:drug_drug]->(d2:drug)
      WHERE toLower(d1.node_name) = "adalimumab"
      RETURN DISTINCT d1.node_name AS Drug, d2.node_name AS InteractingDrug, 
            r.display_relation AS InteractionType
      ORDER BY d2.node_name
    
    If no results, try alternative terms related to the query and explain your reasoning.
    """
    if current_app and current_app.debug:
        print("SYSTEM PROMPT: ", system_prompt)
    return self._create_chain(schema_info, system_prompt, self.prime_kg_graph, return_direct=True, return_intermediate_steps=True, verbose=True)

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
    if current_app and current_app.debug:
        print("SYSTEM PROMPT: ", system_prompt)
    return self._create_chain(self.ckg_schema_info, system_prompt, self.clinical_kg_graph, verbose=False)

  def _extract_kg_result(self, response):
    """
    Helper to extract the kg_result from a chain response dict.
    """
    if isinstance(response, dict):
        return response.get("kg_result")
    return None

  def run_kg_query_with_retries(self, user_query: str, chain, cypher_generator, max_attempts: int = 3, readable_filename: str = None):
    """
    Generic retry logic for KG queries using LangGraph. Tries up to max_attempts, generating a new Cypher query each time.

    Args:
        user_query (str): The user's natural language query.
        chain: The GraphCypherQAChain to use (e.g., self.prime_kg_chain or self.ckg_chain).
        cypher_generator (Callable): Function to generate a Cypher query for each attempt.
        max_attempts (int): Maximum number of attempts.
        readable_filename (str): The name of the KG being queried.

    Returns:
        dict: The final state after running the LangGraph, including queries tried and results.

    Note:
        This method is synchronous. If you want to use it in an async context, call it with asyncio.to_thread or refactor for async support.
    """
    def query_node(state: KGQueryState):
        cypher_query_prompt = cypher_generator(state["user_query"], state["attempt"])
        if current_app and current_app.debug:
            print(f"[DEBUG][KG] Attempt {state['attempt']} - Generated Cypher Query Prompt: {cypher_query_prompt}")
        try:
            result = chain.invoke({"query": cypher_query_prompt})
            if current_app and current_app.debug:
                print(f"[DEBUG][KG] Chain result (type: {type(result)}): {result}")
            cypher_query_actual = cypher_query_prompt  # fallback
            if isinstance(result, dict):
                steps = result.get("intermediate_steps")
                if steps and isinstance(steps, list):
                    for step in steps:
                        if isinstance(step, dict) and "query" in step and isinstance(step["query"], str):
                            cypher_query_actual = step["query"].strip()
                            # Remove 'cypher\n' prefix if present
                            if cypher_query_actual.lower().startswith("cypher"):
                                cypher_query_actual = cypher_query_actual.split("\n", 1)[-1].strip()
                            break
        except Exception as e:
            if current_app and current_app.debug:
                print(f"[ERROR][KG] Exception in chain.invoke: {e}")
            import traceback
            traceback.print_exc()
            result = {}
            cypher_query_actual = cypher_query_prompt

        return {
            "queries_tried": state["queries_tried"] + [cypher_query_actual],
            "results": result,
            "attempt": state["attempt"] + 1,
        }

    def should_retry(state: KGQueryState):
        results = state["results"]
        if current_app and current_app.debug:
            print(f"[DEBUG][KG] should_retry called. Attempt: {state['attempt']}, Results: {results}")
        # Only return success if the 'result' field in the results dict is non-empty
        if isinstance(results, dict) and results.get("result"):
            if current_app and current_app.debug:
                print("[DEBUG][KG] should_retry: Success condition met.")
            return "success"
        elif state["attempt"] < state["max_attempts"]:
            if current_app and current_app.debug:
                print("[DEBUG][KG] should_retry: Retrying...")
            return "query_node"
        else:
            if current_app and current_app.debug:
                print("[DEBUG][KG] should_retry: Max attempts reached. Failing.")
            return "fail"

    builder = StateGraph(KGQueryState)
    builder.add_node("query_node", query_node)
    builder.add_conditional_edges("query_node", should_retry, {
        "success": END,
        "fail": END,
        "query_node": "query_node"
    })
    builder.add_edge(START, "query_node")
    graph = builder.compile()

    initial_state = KGQueryState(
        user_query=user_query,
        attempt=0,
        queries_tried=[],
        results={},
        max_attempts=max_attempts,
    )
    result = graph.invoke(initial_state)
    # If after all attempts there are no results, return minimal structure
    results = result.get("results")
    if not results or (isinstance(results, dict) and not results.get("result")):
        return {
            "kg_result": None,
            "text": ""
        }
    # If successful, extract the result and summary if possible
    if isinstance(results, dict) and "result" in results:
        return {
            "kg_result": results["result"],
            "text": ""  # summary will be added in the service layer
        }
    return result

  def run_primekg_query_with_retries(self, user_query: str, max_attempts: int = 3):
    """
    Retry-enabled PrimeKG query using LangGraph. Tries up to max_attempts with different Cypher strategies.

    Args:
        user_query (str): The user's natural language query.
        max_attempts (int): Maximum number of attempts.

    Returns:
        dict: The final state after running the LangGraph, including queries tried and results.
    """
    return self.run_kg_query_with_retries(
        user_query=user_query,
        chain=self.prime_kg_chain,
        cypher_generator=generate_primekg_cypher,
        max_attempts=max_attempts,
        readable_filename="PrimeKG",
    )

  def run_clinicalkg_query_with_retries(self, user_query: str, max_attempts: int = 3):
    """
    Retry-enabled Clinical KG query using LangGraph. Tries up to max_attempts with different Cypher strategies.

    Args:
        user_query (str): The user's natural language query.
        max_attempts (int): Maximum number of attempts.

    Returns:
        dict: The final state after running the LangGraph, including queries tried and results.
    """
    return self.run_kg_query_with_retries(
        user_query=user_query,
        chain=self.ckg_chain,
        cypher_generator=generate_clinicalkg_cypher,
        max_attempts=max_attempts,
        readable_filename="ClinicalKG",
    )

  def generate_openai_summary(self, user_query: str, kg_result: list) -> str:
    """
    Generate a summary using the OpenAI API, given the user query and the first 5 results from the KG.
    """
    from openai import OpenAI
    client = OpenAI(api_key=self.vlads_openai_key)
    # Truncate or format kg_result for prompt if it's very large
    context_str = str(kg_result[:5])[:4000]  # use first 5 results, adjust as needed for token limits

    prompt = (
        f"User question: {user_query}\n"
        f"Knowledge graph results: {context_str}\n"
        "Please provide a concise, readable summary of the key findings that answer the user's question."
    )

    response = client.chat.completions.create(
        model="gpt-4o",  # or "gpt-3.5-turbo" if you want to save cost
        messages=[{"role": "user", "content": prompt}],
        max_tokens=256,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip()

  def getPrimeKGContexts(self, user_query: str) -> dict:
    try:
        response = self.run_primekg_query_with_retries(user_query)
        kg_result = self._extract_kg_result(response)
        if kg_result:
            summary = self.generate_openai_summary(user_query, kg_result)
            return {"kg_result": kg_result, "text": summary}
        else:
            return {"kg_result": None, "text": ""}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"kg_result": None, "text": ""}

  def getClinicalKGContexts(self, user_query: str) -> dict:
    try:
        response = self.run_clinicalkg_query_with_retries(user_query)
        kg_result = self._extract_kg_result(response)
        if kg_result:
            summary = self.generate_openai_summary(user_query, kg_result)
            return {"kg_result": kg_result, "text": summary}
        else:
            return {"kg_result": None, "text": ""}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"kg_result": None, "text": ""}
