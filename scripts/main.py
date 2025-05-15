import yaml
import psycopg2
import mysql.connector
import os
import argparse
import re
from datetime import datetime, timedelta


CONFIG_PATH = "../config/config.yaml"
TS_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)")

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def connect_postgres(cfg):
    return psycopg2.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database']
    )

def connect_mysql(cfg):
    return mysql.connector.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database']
    )

def parse_mysql_log_and_explain(config, start_time=None, end_time=None):
    log_file = config['mysql']['log_file']

    mysql_conn = connect_mysql(config['mysql'])
    pg_conn = connect_postgres(config['postgres'])

    seen_queries = set()

    print(f"Reading log from: {log_file}")
    with open(log_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if "Query" in line:
            match = TS_REGEX.match(line)
            if not match:
                continue
            try:
                ts = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                continue

            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue

            # Ambil bagian setelah "Query"
            try:
                query = line.split("Query", 1)[-1].strip()
            except:
                continue

            # Skip jika itu EXPLAIN
            if query.lower().startswith("explain "):
                continue

            process_query(query, seen_queries, mysql_conn, pg_conn)

def process_query(query, seen_queries, mysql_conn, pg_conn):
    if query.lower().startswith(("set ", "use ", "show ", "select database()", "begin", "commit")):
        return
    if not query.lower().startswith(("select", "update", "delete")):
        return
    blacklist_keywords = ["@@", "mysql.plugin", "information_schema", "performance_schema"]
    if any(bad in query.lower() for bad in blacklist_keywords):
        return
    if query in seen_queries:
        return
    seen_queries.add(query)

    print(f"Processing: {query}")

    pg_cur = pg_conn.cursor()
    pg_cur.execute(
        "INSERT INTO query_raw (engine, query) VALUES (%s, %s) RETURNING id",
        ("mysql", query)
    )
    query_id = pg_cur.fetchone()[0]
    pg_conn.commit()

    try:
        cur = mysql_conn.cursor()
        cur.execute(f"EXPLAIN {query}")
        explain_result = cur.fetchall()
        columns = [col[0] for col in cur.description]
        explain_data = [dict(zip(columns, row)) for row in explain_result]

        index_scan_count = sum(1 for row in explain_data if row.get("key") not in (None, 'NULL'))
        full_scan_count = sum(1 for row in explain_data if row.get("key") in (None, 'NULL'))
        table_used = explain_data[0].get("table")
        index_used_name = explain_data[0].get("key")
        rows_estimate = explain_data[0].get("rows")

        is_index_used = full_scan_count == 0

        pg_cur.execute(
            """
            INSERT INTO query_analysis (
                query_id, explain_result, is_index_used,
                table_used, index_used, rows_estimate,
                index_scan_count, full_scan_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (query_id, json.dumps(explain_data), is_index_used,
             table_used, index_used_name, rows_estimate,
             index_scan_count, full_scan_count)
        )
        pg_conn.commit()
        print("EXPLAIN saved.")
    except Exception as e:
        print(f"EXPLAIN error: {e}")

def main():
    config = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="Start time (e.g., '2025-05-15 08:00:00')")
    parser.add_argument("--end", type=str, help="End time (e.g., '2025-05-15 09:00:00')")
    parser.add_argument("--last-minutes", type=int, help="Process logs in last N minutes")
    args = parser.parse_args()

    start_time = None
    end_time = None

    if args.last_minutes:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=args.last_minutes)
    else:
        if args.start:
            start_time = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        if args.end:
            end_time = datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")

    print("Select DB engine to check:")
    print("1. MySQL")
    choice = input("Enter choice (1): ")

    if choice == '1':
        parse_mysql_log_and_explain(config, start_time, end_time)
    else:
        print("Invalid or unsupported choice.")

if __name__ == "__main__":
    import json
    main()
