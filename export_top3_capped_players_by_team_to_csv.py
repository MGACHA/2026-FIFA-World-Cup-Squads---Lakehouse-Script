from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import tempfile

# Full script: Top 3 Most Capped Players in Each Team + CSV export
# Source table expected in Lakehouse: worldcup_squads_all

spark = SparkSession.builder.getOrCreate()

TABLE_NAME = "worldcup_squads_all"
LOCAL_EXPORT_DIR = "Data"
LOCAL_LONG_CSV = os.path.join(LOCAL_EXPORT_DIR, "top3_most_capped_players_by_team.csv")
LOCAL_PIVOT_CSV = os.path.join(LOCAL_EXPORT_DIR, "top3_most_capped_players_by_team_pivot.csv")

LAKEHOUSE_EXPORT_DIR = "Files/worldcup_squads_all_csv/exports"
LAKEHOUSE_LONG_CSV = f"{LAKEHOUSE_EXPORT_DIR}/top3_most_capped_players_by_team.csv"
LAKEHOUSE_PIVOT_CSV = f"{LAKEHOUSE_EXPORT_DIR}/top3_most_capped_players_by_team_pivot.csv"
LAKEHOUSE_FILES_DIR = LAKEHOUSE_EXPORT_DIR

TMP_LONG_CSV = os.path.join(tempfile.gettempdir(), "top3_most_capped_players_by_team.csv")
TMP_PIVOT_CSV = os.path.join(tempfile.gettempdir(), "top3_most_capped_players_by_team_pivot.csv")


def get_notebookutils():
    nb = globals().get("notebookutils")
    if nb is not None:
        return nb
    try:
        import notebookutils as nb_mod  # type: ignore
        return nb_mod
    except Exception:
        return None


# 1) Load source table and resolve caps column
source = spark.table(TABLE_NAME)
caps_candidates = ["caps", "apps", "appearances"]
existing_caps_cols = [c for c in caps_candidates if c in source.columns]

if not existing_caps_cols:
    raise ValueError(
        "No caps column found. Expected one of: caps, apps, appearances. "
        f"Available columns: {source.columns}"
    )

caps_col = existing_caps_cols[0]
print(f"Using caps column: {caps_col}")


# 2) Parse caps and rank top 3 within each country
caps_base = (
    source
    .withColumn(
        "caps_num",
        F.regexp_extract(
            F.coalesce(F.col(caps_col).cast("string"), F.lit("")),
            r"(\d+)",
            1,
        ).cast("int"),
    )
    .filter(F.col("caps_num").isNotNull())
    .select("group", "country", "player", "caps_num")
)

w = Window.partitionBy("country").orderBy(F.desc("caps_num"), F.asc("player"))

top_within_team = (
    caps_base
    .withColumn("rank_in_country", F.row_number().over(w))
    .filter(F.col("rank_in_country") <= 3)
    .orderBy(F.asc("country"), F.asc("rank_in_country"))
)

print("Top 3 Most Capped Players in Each Team:")
top_within_team.select("country", "rank_in_country", "player", "caps_num").show(300, truncate=False)


# 3) Build optional compact pivot view (Top 1/2/3 per country)
pdf_team = top_within_team.select("country", "rank_in_country", "player", "caps_num").toPandas()
pdf_team["player_caps"] = pdf_team["player"] + " (" + pdf_team["caps_num"].astype(str) + ")"

pivot = (
    pdf_team
    .pivot(index="country", columns="rank_in_country", values="player_caps")
    .rename(columns={1: "Top 1", 2: "Top 2", 3: "Top 3"})
    .reset_index()
    .sort_values("country")
)

print("\nCompact pivot preview:")
print(pivot.head(10).to_string(index=False))


# 4) Export to local Data/ folder (useful for GitHub repo files)
os.makedirs(LOCAL_EXPORT_DIR, exist_ok=True)
pdf_team.to_csv(LOCAL_LONG_CSV, index=False)
pivot.to_csv(LOCAL_PIVOT_CSV, index=False)

print(f"Local CSV written: {LOCAL_LONG_CSV}")
print(f"Local CSV written: {LOCAL_PIVOT_CSV}")


# 5) Export to Lakehouse Files (for Fabric download UI), if notebookutils is available
nbutils = get_notebookutils()
if nbutils is not None:
    pdf_team.to_csv(TMP_LONG_CSV, index=False)
    pivot.to_csv(TMP_PIVOT_CSV, index=False)

    # Ensure target export directory exists in Lakehouse Files.
    try:
        nbutils.fs.mkdirs(LAKEHOUSE_EXPORT_DIR)
    except Exception:
        # mkdirs may fail if directory already exists in some runtimes.
        pass

    nbutils.fs.cp(f"file:{TMP_LONG_CSV}", LAKEHOUSE_LONG_CSV, True)
    nbutils.fs.cp(f"file:{TMP_PIVOT_CSV}", LAKEHOUSE_PIVOT_CSV, True)

    print(f"Lakehouse CSV written: {LAKEHOUSE_LONG_CSV}")
    print(f"Lakehouse CSV written: {LAKEHOUSE_PIVOT_CSV}")
    print(f"Lakehouse export folder: {LAKEHOUSE_EXPORT_DIR}")

    # Verification block: list files currently visible in attached Lakehouse Files.
    try:
        files_now = nbutils.fs.ls(LAKEHOUSE_FILES_DIR)
        names_now = [f.name for f in files_now]

        print("\nLakehouse Files listing (current attachment):")
        for n in sorted(names_now):
            print(f"- {n}")

        print("\nVerification:")
        print(
            f"- {os.path.basename(LAKEHOUSE_LONG_CSV)} visible: "
            f"{os.path.basename(LAKEHOUSE_LONG_CSV) in names_now}"
        )
        print(
            f"- {os.path.basename(LAKEHOUSE_PIVOT_CSV)} visible: "
            f"{os.path.basename(LAKEHOUSE_PIVOT_CSV) in names_now}"
        )
    except Exception as verify_exc:
        print("Could not list Lakehouse Files for verification.")
        print(f"Details: {verify_exc}")
else:
    print("notebookutils not available; skipped Lakehouse Files export.")

print(f"Rows exported (long): {len(pdf_team):,}")
print(f"Rows exported (pivot): {len(pivot):,}")
