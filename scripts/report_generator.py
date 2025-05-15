import yaml
import psycopg2
from datetime import datetime, timedelta
import os
import json
import argparse

CONFIG_PATH = "../config/config.yaml"

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

def load_data_from_postgres(start_time, end_time):
    config = load_config()
    conn = connect_postgres(config['postgres'])
    cur = conn.cursor()

    cur.execute("""
        SELECT qr.query, qa.table_used, qa.index_used, qa.rows_estimate,
               qa.index_scan_count, qa.full_scan_count, qr.captured_at
        FROM query_analysis qa
        JOIN query_raw qr ON qa.query_id = qr.id
        WHERE qr.captured_at BETWEEN %s AND %s
        ORDER BY qr.captured_at
    """, (start_time, end_time))
    return cur.fetchall()

def load_data_from_json(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return [(d["query"], d["table_used"], d["index_used"], d["rows_estimate"],
             d["index_scan_count"], d["full_scan_count"], d["captured_at"]) for d in data]

def generate_markdown_summary(data_rows, label):
    total = len(data_rows)
    risky = 0
    fullscan_rows = []
    mixedscan_rows = []
    top_rows = []

    for row in data_rows:
        query, table, index, rows_est, ix_count, fs_count, ts = row
        if fs_count and fs_count > 0:
            risky += 1
            fullscan_rows.append((table, query, rows_est, ts))
        if ix_count and fs_count and fs_count > 0:
            mixedscan_rows.append((query, ix_count, fs_count))
        top_rows.append((query, rows_est or 0))

    top_rows = sorted(top_rows, key=lambda x: -x[1])[:5]

    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    filename = f"../output/summary_{label}_{now}.md"
    os.makedirs("../output", exist_ok=True)

    with open(filename, "w") as f:
        f.write(f"# Query Check Summary ({label})\n\n")

        f.write("## 🟥 Full Scan (Tanpa Index)\n")
        f.write("| Table | Query | Rows | Timestamp |\n")
        f.write("|-------|--------|-------|------------|\n")
        for table, query, rows_est, ts in fullscan_rows:
            f.write(f"| {table or '-'} | `{query[:40]}...` | {rows_est} | {ts} |\n")

        f.write("\n## 🟧 Mixed Index (JOIN Sebagian Pakai Index)\n")
        f.write("| Query | Index Scan | Full Scan |\n")
        f.write("|-------|-------------|------------|\n")
        for query, ix, fs in mixedscan_rows:
            f.write(f"| `{query[:50]}...` | {ix} | {fs} |\n")

        f.write("\n## 🟨 Top 5 Rows Estimate\n")
        f.write("| Query | Rows Estimate |\n")
        f.write("|--------|----------------|\n")
        for query, est in top_rows:
            f.write(f"| `{query[:50]}...` | {est} |\n")

        f.write(f"\n✅ Total Query Diperiksa: {total}\n")
        f.write(f"❌ Query Berisiko (full scan): {risky}\n")

    print(f"✅ Report saved to: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--last-minutes", type=int, help="Interval to pull data from DB")
    parser.add_argument("--json", type=str, help="Path to analysis_result.json")
    args = parser.parse_args()

    if args.json:
        data_rows = load_data_from_json(args.json)
        generate_markdown_summary(data_rows, "json")
    elif args.last_minutes:
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=args.last_minutes)
        data_rows = load_data_from_postgres(start_time, end_time)
        generate_markdown_summary(data_rows, "postgres")
    else:
        print("❌ You must specify either --last-minutes or --json")

