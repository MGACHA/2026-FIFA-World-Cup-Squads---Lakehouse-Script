# 2026 FIFA World Cup Squads Analysis (Fabric + PySpark)

## Overview
This project builds a reusable data pipeline and analysis story for the 2026 FIFA World Cup squads.

It starts by scraping and standardizing squad data, saves it to a Lakehouse Delta table, and then runs multiple PySpark analysis scripts with visual outputs.

Primary dataset table:
- `worldcup_squads_all`

Primary notebook:
- `worldcup_squads2026.ipynb`

---

## What This Project Covers
1. Data ingestion from Wikipedia squad tables
2. Cleaning and normalization for analytics (safe column names, structured fields)
3. Storage into Lakehouse Delta table
4. Analysis scripts for age, caps, and goals trends
5. GitHub-ready markdown reports with image references
6. CSV export for source publishing

---

## Repository Structure

```text
GitHub/
  README.md
  worldcup_squads2026.ipynb
  worldcup-analyses-index.md

  lakehouse-setup-and-df-load.md
  age-range-summary-oldest-youngest.md
  average-age-by-country.md
  average-age-by-country-grouped.md
  youngest-vs-oldest-squads.md
  average-age-by-group.md
  most-capped-players-tournament-leaders.md
  top-3-capped-players-in-each-team.md
  age-vs-caps-by-position.md
  top-5-goals-by-position.md

  export_worldcup_squads_to_csv.py

  Data/
    worldcup_squads_all.csv

  images/
    IMAGE_MANIFEST.md
    01-lakehouse-setup-a.png
    ...
    10-top-5-goals-by-position-b.png
```

---

## Tech Stack
- Microsoft Fabric Lakehouse
- PySpark (Spark SQL/DataFrame API)
- pandas
- matplotlib
- requests + BeautifulSoup

---

## End-to-End Workflow

### 1) Build Lakehouse table
Run the setup script section in:
- `lakehouse-setup-and-df-load.md`

Result:
- Lakehouse Delta table `worldcup_squads_all`

### 2) Run analysis sections
Use the notebook or the individual markdown script files:
- `worldcup_squads2026.ipynb`
- or scripts in each `*.md` analysis file

### 3) Export source CSV for GitHub
Run:
- `export_worldcup_squads_to_csv.py`

Result:
- `Data/worldcup_squads_all.csv` (or Lakehouse `Files/` path, depending on runtime)

### 4) Publish analysis docs
- Use `worldcup-analyses-index.md` as the entry point
- Keep charts/tables in `images/`

---

## Analysis Story (In Order)
1. `lakehouse-setup-and-df-load.md`
2. `age-range-summary-oldest-youngest.md`
3. `average-age-by-country.md`
4. `average-age-by-country-grouped.md`
5. `youngest-vs-oldest-squads.md`
6. `average-age-by-group.md`
7. `most-capped-players-tournament-leaders.md`
8. `top-3-capped-players-in-each-team.md`
9. `age-vs-caps-by-position.md`
10. `top-5-goals-by-position.md`

---

## Data Source
- Wikipedia: 2026 FIFA World Cup squads page

Notes:
- This project is intended for educational/analysis use.
- Source content may change over time; reruns can produce updated outputs.

---

## How to Run (Fabric Notebook)
1. Open `worldcup_squads2026.ipynb`
2. Attach to your Lakehouse
3. Run setup/ingestion section first (creates `worldcup_squads_all`)
4. Run analysis sections
5. Export CSV using `export_worldcup_squads_to_csv.py`

---

## Local Development (Optional)
If running helper scripts locally (outside Fabric), install dependencies:

```bash
pip install requests beautifulsoup4 pandas lxml matplotlib openpyxl pyspark
```

Note:
- Full Lakehouse write/read steps require Fabric runtime and Lakehouse context.

---

## Image Convention
All analysis image links use one naming pattern:
- `images/NN-analysis-slug-letter.png`

See:
- `images/IMAGE_MANIFEST.md`

---

## Quality and Reproducibility Notes
- Column names are normalized before Delta write.
- Caps/goals fields are parsed defensively from text.
- Position and age fields are normalized for consistent grouping.
- Some exports have runtime-specific behavior in Fabric paths; script includes safe fallback logic.

---

## Future Improvements
- Add automatic tests for schema assumptions
- Add a data dictionary for key fields
- Add CI checks for markdown/image link validity
- Add notebook-to-markdown export automation

---

## License
Add a license file if you plan to publish publicly (for example, MIT).

---

## Author Notes
This repository contains your World Cup squads analytics workflow prepared for GitHub publishing, with separated scripts, organized outputs, and source data export.
