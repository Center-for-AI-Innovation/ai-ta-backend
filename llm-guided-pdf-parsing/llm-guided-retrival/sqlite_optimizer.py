# Generating a Python script for SQLite performance optimization

sqlite_optimization_script = """
import sqlite3

def optimize_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Set WAL mode
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # Increase cache size
    cursor.execute("PRAGMA cache_size = 10000;")  # Adjust the value based on your available memory
    
    # Set synchronous mode to NORMAL or OFF
    cursor.execute("PRAGMA synchronous = NORMAL;")
    # cursor.execute("PRAGMA synchronous = OFF;")  # Uncomment this line if you prefer OFF mode
    
    # Enable memory-mapped I/O
    cursor.execute("PRAGMA mmap_size = 268435456;")  # Set an appropriate size based on your system
    
    # Print current settings to verify
    cursor.execute("PRAGMA journal_mode;")
    print(f"Journal Mode: {cursor.fetchone()[0]}")
    
    cursor.execute("PRAGMA cache_size;")
    print(f"Cache Size: {cursor.fetchone()[0]}")
    
    cursor.execute("PRAGMA synchronous;")
    print(f"Synchronous: {cursor.fetchone()[0]}")
    
    cursor.execute("PRAGMA mmap_size;")
    print(f"Mmap Size: {cursor.fetchone()[0]}")
    
    conn.close()

if __name__ == "__main__":
    db_path = "/home/guest/ai-ta-backend/UIUC_Chat/pdf-parsing/articles-test.db"  # Replace with your SQLite database path
    optimize_sqlite(db_path)
"""

# Save the script to a Python file
# script_path = "/mnt/data/optimize_sqlite.py"
# with open(script_path, "w") as file:
#     file.write(sqlite_optimization_script)

# script_path
