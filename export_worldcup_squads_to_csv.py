from pyspark.sql import SparkSession
import os
import tempfile

# PySpark export script for Fabric/Lakehouse
# Goal: export worldcup_squads_all to CSV so it can be uploaded to GitHub as source data.

spark = SparkSession.builder.getOrCreate()

TABLE_NAME = "worldcup_squads_all"
OUTPUT_FOLDER = "Files/worldcup_squads_all_csv"   # Spark CSV output folder in Lakehouse
OUTPUT_SINGLE_FILE = "Files/worldcup_squads_all.csv"  # Single CSV in Lakehouse Files
TEMP_LOCAL_SINGLE_FILE = os.path.join(tempfile.gettempdir(), "worldcup_squads_all.csv")
LOCAL_FALLBACK_SINGLE_FILE = "worldcup_squads_all.csv"


def get_notebookutils():
    nb = globals().get("notebookutils")
    if nb is not None:
        return nb
    try:
        import notebookutils as nb_mod  # type: ignore
        return nb_mod
    except Exception:
        return None

# 1) Load Lakehouse table
source = spark.table(TABLE_NAME)

# 2) Export with Spark (scales better, writes folder with part files)
#    Keep this optional because some runtimes block this path operation.
try:
    (
        source
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUTPUT_FOLDER)
    )
    print(f"Spark CSV folder written: {OUTPUT_FOLDER}")
except Exception as exc:
    print("Spark folder export skipped due to runtime path limitation.")
    print(f"Details: {exc}")

# 3) Export true single CSV file (easy GitHub upload)
#    Suitable for this dataset size. For very large data, use OUTPUT_FOLDER above.
pdf = source.toPandas()
nbutils = get_notebookutils()

# Write locally first, then copy to Lakehouse Files so it appears in the Fabric UI.
pdf.to_csv(TEMP_LOCAL_SINGLE_FILE, index=False)

if nbutils is not None:
    nbutils.fs.cp(f"file:{TEMP_LOCAL_SINGLE_FILE}", OUTPUT_SINGLE_FILE, True)
    single_file_path = OUTPUT_SINGLE_FILE
else:
    pdf.to_csv(LOCAL_FALLBACK_SINGLE_FILE, index=False)
    single_file_path = LOCAL_FALLBACK_SINGLE_FILE
    print("notebookutils not available; wrote CSV locally instead of Lakehouse Files.")

print(f"Single CSV file written: {single_file_path}")
print(f"Rows exported: {len(pdf):,}")
