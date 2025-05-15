import yaml
import mysql.connector
import os
import re
import json
import argparse
from datetime import datetime, timedelta, timezone
import subprocess

CONFIG_PATH = "../config/config.yaml"
TS_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)")

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

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
    seen_queries = set()
    result_data = []

    print(f"Reading log from: {log_file}")
    with open(log_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if "Query" in line and "EXPLAIN" not in line:
            match = TS_REGEX.match(line)
            if not match:
                continue
            try:
                ts = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            if start_time and ts < start_time:
                continue
            if end_time and ts > end_time:
                continue

            query = line.strip().split("Query", 1)[-1].strip()

            if query.lower().startswith(("set ", "use ", "show ", "select database()", "begin", "commit")):
                continue
            if not query.lower().startswith(("select", "update", "delete")):
                continue
            blacklist_keywords = ["@@", "mysql.plugin", "information_schema", "performance_schema"]
            if any(bad in query.lower() for bad in blacklist_keywords):
                continue
            if query in seen_queries:
                continue
            seen_queries.add(query)

            print(f"Processing: {query}")

            try:
                cur = mysql_conn.cursor()
                cur.execute(f"EXPLAIN {query}")
                explain_result = cur.fetchall()
                columns = [col[0] for col in cur.description]
                explain_data = [dict(zip(columns, row)) for row in explain_result]

                index_used = all(row.get('key') not in (None, 'NULL') for row in explain_data)
                index_scan_count = sum(1 for row in explain_data if row.get("key") not in (None, 'NULL'))
                full_scan_count = sum(1 for row in explain_data if row.get("key") in (None, 'NULL'))
                table_used = explain_data[0].get("table")
                rows_estimate = explain_data[0].get("rows")

                result_data.append({
                    "query": query,
                    "table_used": table_used,
                    "index_used": explain_data[0].get("key"),
                    "rows_estimate": rows_estimate,
                    "index_scan_count": index_scan_count,
                    "full_scan_count": full_scan_count,
                    "captured_at": ts.isoformat()
                })
                print("EXPLAIN captured.")
            except Exception as e:
                print(f"EXPLAIN error: {e}")
                continue

    os.makedirs("../output", exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    json_file = f"../output/analysis_result_{now}.json"
    with open(json_file, "w") as f:
        json.dump(result_data, f, indent=2)
    print(f"✅ Analysis saved to {json_file}")

    try:
        subprocess.run(["python3", "report_generator.py", "--json", json_file], check=True)
    except Exception as e:
        print(f"❌ Failed to generate report: {e}")

def main():
    config = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--last-minutes", type=int, default=60)
    args = parser.parse_args()

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=args.last_minutes)

    parse_mysql_log_and_explain(config, start_time, end_time)

if __name__ == "__main__":
    main()
