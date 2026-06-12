# Lakehouse Setup and DataFrame Build

## What this script does
This is the first script that creates the Lakehouse-ready dataset and saves it as a Delta table.

1. Installs required Python packages
2. Downloads the 2026 World Cup squads page
3. Extracts squad tables by Group and Country
4. Cleans column names for Delta compatibility
5. Builds one combined DataFrame
6. Saves table to Lakehouse as worldcup_squads_all

## Output
- One Delta table in Lakehouse: worldcup_squads_all
- Printed run summary with rows, columns, and number of squad tables kept

## Findings
This script is the ingestion foundation used by all later analysis scripts in the notebook.

## Image placeholders
![Lakehouse SetUp - Attach notebook to Lakehouse](images/01-lakehouse-setup-a.png)

![Lakehouse Table Created - Data Preview](images/01-lakehouse-setup-b.png)

## Script
```python
# Install required packages for scraping, HTML parsing, and table extraction.
%pip install pandas lxml beautifulsoup4 requests
```

```python
# Standard library: regex is used to clean/sanitize column names for Delta compatibility.
import re

# Third-party libraries used for web request + HTML parsing + table parsing.
import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

# Fabric notebooks expose a Spark session as spark when attached to a Lakehouse.
# We fail early with a clear message if Spark is unavailable.
if "spark" not in globals():
    raise NameError("Spark session is not available. Attach notebook to Lakehouse and run again.")

# Target page containing World Cup squad tables.
url = "https://en.wikipedia.org/wiki/2026_FIFA_World_Cup_squads"

# Request headers make the HTTP call look like a real browser request.
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

# Download the page HTML and stop immediately if the request fails.
response = requests.get(url, headers=headers, timeout=30)
response.raise_for_status()
html = response.text

# pandas reads all <table> tags into DataFrames.
raw_tables = pd.read_html(StringIO(html))

# BeautifulSoup parses the same HTML so we can navigate headings above each table.
soup = BeautifulSoup(html, "lxml")
html_tables = soup.find_all("table")

# Helper: for a given table tag, find the nearest heading of a specific level above it.
# We use this to capture group (h2) and country (h3) labels.
def find_heading_above(tag, level):
    for heading in tag.find_all_previous([level]):
        text = heading.get_text(separator=" ", strip=True)
        if text:
            return text
    return ""

# Helper: detect whether a parsed table looks like a squad roster table.
def is_squad_table(df):
    cols = {str(c).strip().lower() for c in df.columns}

    # Core roster columns expected in squad tables.
    required_any = {"no.", "pos.", "player", "club"}
    return required_any.issubset(cols)

# Build one labelled DataFrame per HTML table.
# Keep ONLY squad tables (group + country + roster columns), skip other statistics tables.
frames = []
for i, (html_table, df) in enumerate(zip(html_tables, raw_tables)):
    group_name = find_heading_above(html_table, "h2")
    country_name = find_heading_above(html_table, "h3")

    # Keep only real squad roster tables for countries inside groups.
    if not group_name.startswith("Group "):
        continue
    if not country_name:
        continue
    if not is_squad_table(df):
        continue

    out = df.copy()  # Copy so we do not mutate the original parsed DataFrame.

    # Insert metadata columns at the front for easier filtering later.
    out.insert(0, "group", group_name)
    out.insert(1, "country", country_name)

    # Keep lineage: identifies which original extracted table each row came from.
    out["source_table_index"] = i
    frames.append(out)

if not frames:
    raise ValueError("No squad tables were detected. Check page structure or filter rules.")

# Stack all labelled squad tables into one analytics-friendly DataFrame.
combined = pd.concat(frames, ignore_index=True)

# Delta Lake does not allow some characters in column names.
# This section converts all column names to safe snake_case and ensures uniqueness.
seen = {}
safe_cols = []
for col in combined.columns:
    # Replace separators/invalid characters with underscore.
    name = re.sub(r"[ ,;{}()\n\t=]+", "_", str(col))
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)

    # Normalize repeated underscores, trim edges, and lowercase.
    name = re.sub(r"_+", "_", name).strip("_").lower()

    # Fallback name if column becomes empty after cleanup.
    if not name:
        name = "col"

    # Delta/Spark column names should not start with a number.
    if name[0].isdigit():
        name = f"col_{name}"

    # If duplicate name appears, append _1, _2, ... to make it unique.
    count = seen.get(name, 0)
    if count > 0:
        final_name = f"{name}_{count}"
    else:
        final_name = name
    seen[name] = count + 1
    safe_cols.append(final_name)

# Apply sanitized names back to the DataFrame.
combined.columns = safe_cols

# Final Lakehouse table name.
table_name = "worldcup_squads_all"

# Convert pandas -> Spark DataFrame, then save as Delta table in Lakehouse.
# mode("overwrite") replaces table contents each run.
# overwriteSchema allows schema updates if columns/types change.
spark.createDataFrame(combined).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# Friendly run summary.
print(f"Saved Lakehouse table: {table_name}")
print(f"Rows: {len(combined):,} | Columns: {combined.shape[1]}")
print(f"Squad tables kept: {len(frames)}")
```
