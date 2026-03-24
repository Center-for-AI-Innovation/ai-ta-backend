#!/usr/bin/env python3
"""
Real-time Pipeline Progress Monitor
Displays progress of retrieval.py with breakdown by date.

Usage:
    python monitor_pipeline.py                    # Monitor default test env
    python monitor_pipeline.py --refresh 10       # Refresh every 10 seconds
    python monitor_pipeline.py --env production   # Monitor production env
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))  # shared/ is one level up (in vyraid-pubmed-daily-update-and-vectorization/)
from shared.database import DB


def clear_screen():
    """Clear terminal screen"""
    os.system('clear' if os.name == 'posix' else 'cls')


def format_duration(seconds):
    """Format seconds into human-readable duration"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds / 3600)
        mins = int((seconds % 3600) / 60)
        return f"{hours}h {mins}m"


def load_pipeline_state(state_file_path):
    """Load current pipeline state from JSON file"""
    if not Path(state_file_path).exists():
        return None
    
    try:
        with open(state_file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}


def get_articles_by_date(db, start_date=None, end_date=None):
    """Query articles grouped by publication_date"""
    query = """
        SELECT 
            DATE(publication_date) as pub_date,
            COUNT(*) as count,
            COUNT(*) FILTER (WHERE vector_indexed = true) as vectorized,
            COUNT(*) FILTER (WHERE vector_indexed = false) as pending_vectorization
        FROM articles
    """
    
    params = []
    if start_date or end_date:
        query += " WHERE "
        conditions = []
        if start_date:
            conditions.append("publication_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("publication_date <= ?")
            params.append(end_date)
        query += " AND ".join(conditions)
    
    query += " GROUP BY DATE(publication_date) ORDER BY pub_date"
    
    return db.conn.execute(query, params).fetchall()


def get_failure_summary(db):
    """Get failure counts by reason"""
    query = """
        SELECT failure_reason, COUNT(*) as count
        FROM xml_failed_downloads
        GROUP BY failure_reason
        ORDER BY count DESC
        LIMIT 10
    """
    return db.conn.execute(query).fetchall()


def get_xml_progress(db):
    """Get XML processing progress"""
    query = """
        SELECT 
            xml_filename,
            total_pmcid_processed,
            processed_all_pmcid,
            last_updated
        FROM xml_processing_log
        ORDER BY xml_filename DESC
        LIMIT 5
    """
    return db.conn.execute(query).fetchall()


def calculate_processing_rate(db):
    """Calculate articles processed per minute over last 5 minutes"""
    query = """
        SELECT COUNT(*) as recent_count
        FROM articles
        WHERE inserted_at >= datetime('now', '-5 minutes')
    """
    try:
        result = db.conn.execute(query).fetchone()
        if result and result[0]:
            return result[0] / 5.0  # articles per minute
    except:
        pass
    return 0


def monitor_loop(db, state_file, refresh_interval, start_date=None, end_date=None):
    """Main monitoring loop"""
    iteration = 0
    
    while True:
        try:
            clear_screen()
            iteration += 1
            
            # Header
            print("=" * 80)
            print(f"  PubMed Pipeline Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            print()
            
            # Load pipeline state
            state = load_pipeline_state(state_file)
            
            if state and 'error' not in state:
                status = state.get('status', 'UNKNOWN')
                status_color = {
                    'RUNNING': '\033[92m',  # Green
                    'COMPLETED': '\033[94m',  # Blue
                    'FAILED': '\033[91m'  # Red
                }.get(status, '')
                reset_color = '\033[0m'
                
                print(f"Pipeline Status: {status_color}{status}{reset_color}")
                
                if state.get('start_time'):
                    start_time = datetime.fromisoformat(state['start_time'])
                    if status == 'RUNNING':
                        elapsed = (datetime.now() - start_time).total_seconds()
                        print(f"Running Time: {format_duration(elapsed)}")
                    elif state.get('end_time'):
                        end_time = datetime.fromisoformat(state['end_time'])
                        total_time = (end_time - start_time).total_seconds()
                        print(f"Total Time: {format_duration(total_time)}")
                
                if state.get('processed_count'):
                    print(f"Processed Count: {state['processed_count']} articles")
                
                if state.get('metrics'):
                    metrics = state['metrics']
                    print(f"Average Times: OA={metrics.get('oa', 0):.2f}s, "
                          f"Download={metrics.get('download', 0):.2f}s, "
                          f"Upload={metrics.get('upload', 0):.2f}s")
                
                print()
            else:
                print("Pipeline Status: NOT STARTED or state file not found")
                print()
            
            # Articles by date
            print("Articles by Publication Date:")
            print("-" * 80)
            print(f"{'Date':<12} {'Total':<8} {'Vectorized':<12} {'Pending':<10} {'Progress':<20}")
            print("-" * 80)
            
            articles_by_date = get_articles_by_date(db, start_date, end_date)
            total_articles = 0
            total_vectorized = 0
            
            if articles_by_date:
                for pub_date, count, vectorized, pending in articles_by_date:
                    total_articles += count
                    total_vectorized += vectorized
                    progress_pct = (vectorized / count * 100) if count > 0 else 0
                    bar_length = 15
                    filled = int(bar_length * progress_pct / 100)
                    bar = '█' * filled + '░' * (bar_length - filled)
                    
                    date_str = pub_date if pub_date else "No date"
                    print(f"{date_str:<12} {count:<8} {vectorized:<12} {pending:<10} {bar} {progress_pct:>5.1f}%")
                
                print("-" * 80)
                print(f"{'TOTAL':<12} {total_articles:<8} {total_vectorized:<12} "
                      f"{total_articles - total_vectorized:<10}")
            else:
                print("No articles found in database yet")
            
            print()
            
            # Processing rate
            rate = calculate_processing_rate(db)
            if rate > 0:
                print(f"Processing Rate: {rate:.1f} articles/minute (last 5 minutes)")
                print()
            
            # Failures
            failures = get_failure_summary(db)
            if failures:
                print("Top Failure Reasons:")
                print("-" * 80)
                for reason, count in failures[:5]:
                    print(f"  {reason:<50} {count:>8} failures")
                print()
            
            # Recent XML progress
            xml_progress = get_xml_progress(db)
            if xml_progress:
                print("Recent XML Files:")
                print("-" * 80)
                for xml_file, processed, completed, last_updated in xml_progress:
                    status_icon = "✓" if completed else "⋯"
                    print(f"  {status_icon} {xml_file:<25} {processed:>6} articles  "
                          f"(updated: {last_updated})")
                print()
            
            # Footer
            print("-" * 80)
            print(f"Refreshing every {refresh_interval}s | Press Ctrl+C to exit | Iteration #{iteration}")
            
            time.sleep(refresh_interval)
            
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped by user")
            break
        except Exception as e:
            print(f"\nError during monitoring: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Monitor PubMed pipeline progress")
    parser.add_argument("--refresh", type=int, default=5, help="Refresh interval in seconds (default: 5)")
    parser.add_argument("--env", choices=['testing', 'production'], default='testing',
                        help="Environment to monitor (default: testing)")
    parser.add_argument("--start-date", type=str, help="Filter by start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Filter by end date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    # Load appropriate environment
    # Path: /home/dadams/pub-med-daily/.env.{testing|production}
    # Current: /home/dadams/pub-med-daily/ai-ta-backend/ai_ta_backend/utils/vyraid-daily-update-and-victorization
    env_file = Path(__file__).parent.parent.parent.parent.parent / f".env.{args.env}"
    if env_file.exists():
        load_dotenv(env_file)
        print(f"Loaded environment from {env_file}")
    else:
        print(f"Warning: {env_file} not found, using current environment")
    
    # Get database connection
    db_dsn = os.getenv('SUPABASE_DB')
    db_schema = os.getenv('DB_SCHEMA', 'vyraid')
    
    if not db_dsn:
        print("Error: SUPABASE_DB environment variable not set")
        sys.exit(1)
    
    # Get state file path
    state_file = os.getenv('PIPELINE_STATE_LOG', 
                          'pipeline_state/retrieval_state.json')
    
    print(f"Monitoring database: schema={db_schema}")
    print(f"State file: {state_file}")
    print(f"Refresh interval: {args.refresh}s")
    if args.start_date or args.end_date:
        print(f"Date filter: {args.start_date or 'earliest'} to {args.end_date or 'latest'}")
    print()
    
    try:
        db = DB(db_dsn, db_schema)
        monitor_loop(db, state_file, args.refresh, args.start_date, args.end_date)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()


if __name__ == "__main__":
    main()
