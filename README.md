# Automatic Query Check

This is project to automate query analysis before deploying to production. It collects query logs, parses them, runs EXPLAIN, and stores structured analysis results to PostgreSQL for dashboarding in Metabase.

## 🚀 Features

- ✅ Parse MySQL general logs automatically
- ✅ Filter query types: `SELECT`, `UPDATE`, `DELETE`
- ✅ Skip internal/system queries (`@@`, `information_schema`, etc.)
- ✅ Detect whether each part of query uses index
- ✅ Track granular EXPLAIN info:
  - `table_used`
  - `index_used`
  - `rows_estimate`
  - `index_scan_count`
  - `full_scan_count`
- ✅ Filter logs by `--start/--end` or `--last-minutes`
- ✅ Output ready for Metabase dashboards

## 🏗️ Folder Structure

```
query_check_poc/
├── config/              # YAML config files
│   └── config.yaml
├── logs/
│   └── mysql/           # Contains general.log
├── scripts/
│   └── main.py          # Main parser script
├── sample_query_list.sql
├── README.md
└── .gitignore
```

## ⚙️ How to Run

```bash
# Activate venv if needed
source venv/bin/activate

# Install deps
pip install -r requirements.txt

# Run parser with time filters
cd scripts
python3 main.py --last-minutes 10
python3 main.py --start "2025-05-15 08:00:00" --end "2025-05-15 09:00:00"
```

## 🛠 PostgreSQL Table Schema

```sql
CREATE TABLE query_raw (
  id SERIAL PRIMARY KEY,
  engine TEXT,
  query TEXT,
  captured_at TIMESTAMP DEFAULT now()
);

CREATE TABLE query_analysis (
  id SERIAL PRIMARY KEY,
  query_id INTEGER REFERENCES query_raw(id),
  explain_result JSONB,
  is_index_used BOOLEAN,
  table_used TEXT,
  index_used TEXT,
  rows_estimate INTEGER,
  index_scan_count INTEGER,
  full_scan_count INTEGER,
  analyzed_at TIMESTAMP DEFAULT now()
);
```

## 📊 Recommended Metabase Cards

1. Query tanpa index
2. Top query dengan full scan
3. Distribusi `is_index_used`
4. Query dengan mix scan
5. Rows estimate tertinggi
6. Jumlah query per hari

## 🔐 .gitignore

```
venv/
__pycache__/
*.pyc
logs/
*.log
*.sqlite
.env
*.db
```

---

