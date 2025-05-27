import os

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_openai import ChatOpenAI


class GraphDatabase:

  def __init__(self):
    # self.clinical_kg_graph = Neo4jGraph(
    #     url=os.environ['CKG_NEO4J_URI'],
    #     username=os.environ['CKG_NEO4J_USERNAME'],
    #     password=os.environ['CKG_NEO4J_PASSWORD'],
    #     database=os.environ['CKG_NEO4J_DATABASE'],
    #     refresh_schema=True,
    # )

    self.prime_kg_graph = Neo4jGraph(
        url=os.environ['PRIME_KG_NEO4J_URI'],
        username=os.environ['PRIME_KG_NEO4J_USERNAME'],
        password=os.environ['PRIME_KG_NEO4J_PASSWORD'],
        database=os.environ['PRIME_KG_NEO4J_DATABASE'],
        refresh_schema=True,
    )

    # Get schema information for the system prompt
    # self.ckg_schema_info = self._get_schema_info(self.clinical_kg_graph)
    self.prime_kg_schema_info = self._get_schema_info(self.prime_kg_graph)

    # Create the chain with the clinical KG system prompt
    # self.ckg_chain = self._create_clinical_kg_chain()
    self.prime_kg_chain = self._create_prime_kg_chain()

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

  def _create_prime_kg_chain(self):
    """Create a GraphCypherQAChain with a prime KG system prompt."""
    schema_info = self.prime_kg_schema_info
    # print("PrimeKG SCHEMA INFO: ", schema_info)
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
    print("SYSTEM PROMPT: ", system_prompt)
    return GraphCypherQAChain.from_llm(
        ChatOpenAI(temperature=0, model="gpt-4.1", api_key=os.environ['VLADS_OPENAI_KEY']),
        return_intermediate_steps=True,
        graph=self.prime_kg_graph,
        verbose=True,
        allow_dangerous_requests=True,
        system_message=system_prompt,
    )

  # extra function to create a chain with a custom prompt
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
        ChatOpenAI(temperature=0, model="gpt-4o", api_key=os.environ['VLADS_OPENAI_KEY']),
        graph=self.clinical_kg_graph,
        verbose=False,
        allow_dangerous_requests=True,
        system_message=system_prompt,
    )
