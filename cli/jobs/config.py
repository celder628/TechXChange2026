import atexit
import os
import tempfile


def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


base_url = require_env("BASE_URL")

spark = {
    "environment": os.environ.get("APP_ENV", "onprem"),
    "username": require_env("WXD_USERNAME"),
    "api_key": require_env("WXD_API_KEY"),
}

_storage_type = os.environ.get("STORAGE_TYPE", "s3")
if _storage_type not in ("s3", "adls"):
    raise RuntimeError(
        f"Invalid STORAGE_TYPE '{_storage_type}': must be 's3' or 'adls'"
    )

if _storage_type == "s3":
    storage = {
        "type": "s3",
        "bucket_name": os.environ.get("S3_BUCKET_NAME", "wx-medallion-backup-test"),
        "source_bucket": os.environ.get("S3_SOURCE_BUCKET", "wx-medallion-src"),
        "source_csv": os.environ.get("SOURCE_CSV", "cfpb/complaints.csv"),
        "endpoint": os.environ.get("S3_ENDPOINT", "https://s3.us-east-1.amazonaws.com"),
        "access_key": require_env("S3_ACCESS_KEY"),
        "secret_key": require_env("S3_SECRET_KEY"),
    }
else:
    storage = {
        "type": "adls",
        "storage_account": require_env("ADLS_STORAGE_ACCOUNT"),
        "account_key": require_env("ADLS_ACCOUNT_KEY"),
        "bucket_name": os.environ.get("ADLS_CONTAINER", "wx-medallion"),
        "source_bucket": os.environ.get("ADLS_SOURCE_CONTAINER", "wx-medallion-src"),
        "source_csv": os.environ.get("SOURCE_CSV", "cfpb/complaints.csv"),
    }

_medallion_table_format = os.environ.get("MEDALLION_TABLE_FORMAT", "iceberg")
if _medallion_table_format not in ("iceberg", "delta"):
    raise RuntimeError(
        f"Invalid MEDALLION_TABLE_FORMAT '{_medallion_table_format}': must be 'iceberg' or 'delta'"
    )

medallion = {
    "table_format": _medallion_table_format,
    "default_catalog": os.environ.get("MEDALLION_DEFAULT_CATALOG", "wx_medallion"),
    "bronze_schema": os.environ.get("MEDALLION_BRONZE_SCHEMA", "cfpb_bronze"),
    "silver_schema": os.environ.get("MEDALLION_SILVER_SCHEMA", "cfpb_silver"),
    "gold_schema": os.environ.get("MEDALLION_GOLD_SCHEMA", "cfpb_gold"),
}

catalog: dict[str, str] = {
    "catalog_name": os.environ.get("CATALOG_NAME", "Medallion Catalog"),
    "connection_name": os.environ.get("CONNECTION_NAME","PrestoConnection"),
}

kafka = None

if os.environ.get("KAFKA_TOPIC"):
    _kafka_security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    if _kafka_security_protocol not in ("SASL_SSL", "SASL_PLAINTEXT", "SSL"):
        raise RuntimeError(
            f"Invalid KAFKA_SECURITY_PROTOCOL '{_kafka_security_protocol}': must be 'SASL_SSL', 'SASL_PLAINTEXT' or 'SSL'"
        )

    def _write_ssl_ca_temp_file():
        pem_content = require_env("KAFKA_SSL_CA_CERT")
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".pem", delete=False, prefix="kafka_ca_"
        )
        tmp.write(pem_content)
        tmp.flush()
        tmp.close()
        atexit.register(os.remove, tmp.name)
        return tmp.name

    kafka = {
        "topic": os.environ.get("KAFKA_TOPIC"),
        "bootstrap_servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security_protocol": _kafka_security_protocol,
        "sasl_mechanism": "PLAIN",
        **(
            {
                "sasl_jaas_config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required"
                    f" username='{require_env('KAFKA_SASL_USERNAME')}'"
                    f" password='{require_env('KAFKA_SASL_PASSWORD')}';"
                )
            }
            if _kafka_security_protocol in ("SASL_SSL", "SASL_PLAINTEXT")
            else {"ssl_ca_location": _write_ssl_ca_temp_file()}
        ),
    }
