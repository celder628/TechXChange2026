# ──────────────────────────────────────────────────────────────────────────────
# IMPORTANT: Do NOT commit real credentials here.
# Copy this file to env.local.sh, fill in secrets there, and source that file.
# env.local.sh is listed in .gitignore and will not be tracked.
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL=cpd-cpd.apps.itz-lqgsjp.infra01-lb.wdc07.techzone.ibm.com
ENGINE_ID=spark625
INSTANCE_ID=1786737641131823
VOLUME_NAME="cpd::spark01-volume1"

# ── Spark ────────────────────────────────────────────────────────────────────
APP_ENV=onprem
WXD_USERNAME=cpadmin
WXD_API_KEY=
AE_SPARK_EXECUTOR_COUNT=1
SPARK_DRIVER_CORES=1
SPARK_DRIVER_MEMORY=4G
SPARK_EXECUTOR_CORES=1
SPARK_EXECUTOR_MEMORY=4G

# ── Storage ───────────────────────────────────────────────────────────────────
# Set STORAGE_TYPE to "s3" or "adls" (default: s3)
STORAGE_TYPE=s3
SOURCE_CSV=cfpb/complaints.csv

# ── S3 credentials (used when STORAGE_TYPE=s3) ───────────────────────────────
S3_BUCKET_NAME=techxchange-data
S3_SOURCE_BUCKET=techxchange-src
S3_ACCESS_KEY=
S3_SECRET_KEY=
S3_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud

# ── Medallion ─────────────────────────────────────────────────────────────────
MEDALLION_TABLE_FORMAT=iceberg
MEDALLION_DEFAULT_CATALOG=wx_medallion
MEDALLION_BRONZE_SCHEMA=cfpb_bronze
MEDALLION_SILVER_SCHEMA=cfpb_silver
MEDALLION_GOLD_SCHEMA=cfpb_gold

# ── Catalog ───────────────────────────────────────────────────────────────────
CATALOG_NAME="Medallion Catalog"
CONNECTION_NAME=PrestoConnection
