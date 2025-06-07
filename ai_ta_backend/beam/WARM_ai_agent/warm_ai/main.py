import os
from dotenv import load_dotenv
from warm_ai.agents.sql_agent import SQLAIAgent
from warm_ai.utils.sql import extract_sql_query

def main():
    print("Starting WARM AI Agent...")

    # Load environment variables
    load_dotenv()
    
    # Build connection string from environment variables
    connection_str = (
        f"Driver={os.getenv('DB_DRIVER')};"
        f"Server={os.getenv('DB_SERVER')};"
        f"Database={os.getenv('DB_NAME')};"
        f"Authentication=ActiveDirectoryIntegrated;"  # Added for Ubuntu
        "Encrypt=yes;"  # Added for Ubuntu
        # f"Trusted_Connection={os.getenv('DB_TRUSTED_CONNECTION')};"
        f"TrustServerCertificate={os.getenv('DB_TRUST_SERVER_CERT')};"
    )
    print("connection_str", connection_str)
    print("Initializing agent with connection string...")
    agent = SQLAIAgent(connection_str, os.getenv('VIKRAM_OPENAI_API_KEY'))
    print("Connecting to database...")
    agent.connect()

    while True:
        # Get user input
        user_query = input("\nEnter your question (or 'quit' to exit): ")
        
        # Check for exit condition
        if user_query.lower() in ['quit', 'exit', 'q']:
            break

        print("\nProcessing natural language query...")
        results = agent.query(user_query)

        if results:
            print("\nProposed response:")
            print(results)
            
            # Extract SQL query if present
            sql_query = extract_sql_query(results)
            if sql_query:
                verify = input("\nWould you like to execute this SQL query? (y/n): ")
                if verify.lower() in ['y', 'yes']:
                    print("\nExecuting query...")
                    results = agent.execute_sql(sql_query)
                    if results:
                        print("\nQuery Results:")
                        for row in results:
                            print(row)

            # Allow user to verify and continue
            # verify = input("\nWould you like to ask another question? (y/n): ")
            # if verify.lower() not in ['y', 'yes']:
            #     break

if __name__ == "__main__":
    main()