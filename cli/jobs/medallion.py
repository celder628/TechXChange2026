# (C) Copyright IBM Corporation 2026.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, localtimestamp, to_json, struct, from_json, col
from pyspark.sql.streaming import StreamingQueryListener
from pathlib import Path
import base64
import requests
import json
import time
from datetime import datetime, date, timedelta, UTC

def read_config():

    import config

    source_bucket = config.storage["source_bucket"]
    source_key = config.storage["source_key"]
    endpoint = config.storage["endpoint"]

    read_location = f"s3a://{source_bucket}/{source_key}"
    spark_src_options = {
        f"spark.hadoop.fs.s3a.bucket.{source_bucket}.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        f"spark.hadoop.fs.s3a.bucket.{source_bucket}.endpoint": endpoint,
        f"spark.hadoop.fs.s3a.bucket.{source_bucket}.access.key": config.storage["access_key"],
        f"spark.hadoop.fs.s3a.bucket.{source_bucket}.secret.key": config.storage["secret_key"],
    }
    kafka_config = None
    if config.kafka:
        kafka_options = {
            "kafka.bootstrap.servers": config.kafka["bootstrap_servers"],
            "kafka.security.protocol": config.kafka["security_protocol"],
            "kafka.delivery.timeout.ms": 500000,
        }
        if config.kafka["security_protocol"] == "SASL_SSL":
            kafka_options["kafka.sasl.mechanism"] = config.kafka["sasl_mechanism"]
            kafka_options["kafka.sasl.jaas.config"] = config.kafka["sasl_jaas_config"]
        else:
            kafka_options["kafka.ssl.ca.location"] = config.kafka["ssl_ca_location"]

        kafka_config = {
            **config.kafka,
            "kafka_options": kafka_options,
        }

    config_values = {
        "base_url": config.base_url,
        "spark": config.spark,
        "storage": {
            **config.storage,
            "read_location": read_location,
            "spark_src_options": spark_src_options,
        },
        "medallion": config.medallion,
        "catalog": config.catalog,
        "kafka": kafka_config,
    }
    
    return config_values


def connect_to_spark(config_data, app_name, default_catalog):

    spark_config = config_data["spark"]
    wxd_username = spark_config["username"]
    wxd_api_key = spark_config["api_key"]
    environment = spark_config["environment"]

    # Set the username for the metastore
    if not wxd_username.startswith("ibmlhapikey_"):
        wxd_hms_username = "ibmlhapikey_" + wxd_username
    else:
        wxd_hms_username = wxd_username

    if environment == "cloud":
        string_to_encode = wxd_hms_username + ":" + wxd_api_key
        encoded_apikey = base64.b64encode(string_to_encode.encode("utf-8")).decode(
            "utf-8"
        )
        wxd_encoded_apikey = "Basic " + encoded_apikey
    else:
        string_to_encode = wxd_username + ":" + wxd_api_key
        encoded_apikey = base64.b64encode(string_to_encode.encode("utf-8")).decode(
            "utf-8"
        )
        wxd_encoded_apikey = "ZenApiKey " + encoded_apikey

    spark_builder = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.hive.metastore.client.plain.username", wxd_hms_username)
        .config("spark.hive.metastore.client.plain.password", wxd_api_key)
        .config("spark.hadoop.wxd.apikey", wxd_encoded_apikey)
        .config("spark.sql.defaultCatalog", default_catalog)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Add options for spark source bucket
    for key, value in config_data["storage"]["spark_src_options"].items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    return spark


def configure_storage(spark, config_data):

    print("Configure the source storage location")

    storage_config = config_data["storage"]
    source_bucket = storage_config["source_bucket"]
    source_url = storage_config["endpoint"]
    access_key = storage_config["access_key"]
    secret_key = storage_config["secret_key"]

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set(f"fs.s3a.bucket.{source_bucket}.endpoint", source_url)
    hadoopConf.set(f"fs.s3a.bucket.{source_bucket}.access.key", access_key)
    hadoopConf.set(f"fs.s3a.bucket.{source_bucket}.secret.key", secret_key)
    hadoopConf.set(
        f"spark.hadoop.fs.s3a.bucket.{source_bucket}.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    return hadoopConf


def create_schema(spark, bucket_name, catalog, schema):
    print(f"Create the {schema} schema in catalog {catalog}")
    # Create a schema
    sql_query = f"""CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} LOCATION 's3a://{bucket_name}/{schema}'"""
    print(sql_query)
    spark.sql(sql_query)
    print()


def list_databases(spark):
    print("Display the schemas in the default catalog")
    # list the database in the default schema
    spark.sql("show databases").show()


def drop_table(spark, table_name):

    # Define the company table
    drop_table_sql = f"""DROP TABLE IF EXISTS {table_name}"""

    # Create the company table
    spark.sql(drop_table_sql)


def ingest_complaint_data(spark, bronze_table, table_format, read_location):

    # Create a schema for the complaint table
    complaint_schema = StructType(
        [
            StructField("date_received", StringType(), True),
            StructField("product", StringType(), True),
            StructField("sub_product", StringType(), True),
            StructField("issue", StringType(), True),
            StructField("sub_issue", StringType(), True),
            StructField("complaint_what_happened", StringType(), True),
            StructField("company_public_response", StringType(), True),
            StructField("company", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("consumer_consent_provided", StringType(), True),
            StructField("submitted_via", StringType(), True),
            StructField("date_sent_to_company", StringType(), True),
            StructField("company_response", StringType(), True),
            StructField("timely", StringType(), True),
            StructField("consumer_disputed", StringType(), True),
            StructField("complaint_id", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("creation_time", TimestampType(), True),
        ]
    )

    print(f"Import the complaints table from {read_location}\n")
 
    start_time = time.time()

    spark.read.csv(
        read_location,
        schema=complaint_schema,
        header=True,
        multiLine=True,
        escape='"',
        inferSchema=False,
    ).withColumn("data_source", lit("csv")).withColumn(
        "creation_time", localtimestamp()
    ).writeTo(
        bronze_table
    ).tableProperty(
        "write.format.default", "parquet"
    ).using(
        table_format
    ).createOrReplace()

    end_time = time.time()

    duration = end_time - start_time

    print(f"\nImport of {bronze_table} completed in {duration} seconds\n")


def retrieve_record_count(spark, table_name):

    print(f"Count of records in the {table_name} table")
    spark.sql(f"select count(1) from {table_name}").show()


def create_company_table(spark, company_table, table_format, source_table):

    drop_table(spark, company_table)

    # Define the company table
    company_table_create = f"""CREATE TABLE IF NOT EXISTS {company_table} (
    company STRING)
    USING {table_format}
    """

    # Create the company table
    spark.sql(company_table_create)

    # Create a select for the unique companyies
    company_sql = f"""SELECT DISTINCT company FROM {source_table}"""

    # Append records to the silver level table
    spark.sql(company_sql).write.mode("overwrite").insertInto(company_table)


def create_state_summary_table(
    spark, summary_by_state_table, table_format, source_table
):

    drop_table(spark, summary_by_state_table)

    # Define the company table
    summary_by_state_table_create = f"""CREATE TABLE IF NOT EXISTS {summary_by_state_table} (
    state STRING,
    count INTEGER)
    USING {table_format}
    """

    # Create the company table
    spark.sql(summary_by_state_table_create)

    # Create a select for the unique companyies
    summary_sql = f"""SELECT
    state, COUNT(1) as count
    FROM
    {source_table}
    GROUP BY
    state
    ORDER BY
        count DESC"""

    # Append records to the silver level table
    spark.sql(summary_sql).write.mode("overwrite").insertInto(summary_by_state_table)


def create_summary_by_year_table(
    spark, summary_by_year_table, level, table_format, source_table
):
    
    drop_table(spark, summary_by_year_table)

    # Define the company table
    summary_by_year_table_create = f"""CREATE TABLE {summary_by_year_table} (
    year STRING,
    count INTEGER)
    USING {table_format}
    """

    # Create the company table
    spark.sql(summary_by_year_table_create)

    select_year = "complaint_year"

    if (level == "bronze"): 
        select_year = "year(date(date_received)) complaint_year"

    # Create a select for the unique companyies
    summary_sql = f"""SELECT
    {select_year}, count(1) count
    FROM
    {source_table}
    GROUP BY 
    complaint_year
    ORDER BY 
    complaint_year DESC"""

    # Append records to the silver level table
    spark.sql(summary_sql).write.mode("overwrite").insertInto(summary_by_year_table)


def create_silver_table(spark, silver_table, table_format):

    print(f"Create the table {silver_table} if it does not exist")

    sql_silver_table_create = f"""CREATE TABLE IF NOT EXISTS {silver_table} (
    complaint_year STRING,
    date_received DATE,
    product STRING,
    sub_product STRING,
    issue STRING,
    sub_issue STRING,
    complaint_what_happened STRING,
    company_public_response STRING,
    company STRING,
    state STRING,
    zip_code STRING,
    tags STRING,
    consumer_consent_provided STRING,
    submitted_via STRING,
    date_sent_to_company DATE,
    company_response STRING,
    timely STRING,
    consumer_disputed STRING,
    complaint_id STRING,
    data_source STRING,
    creation_time TIMESTAMP)
    USING {table_format}
    PARTITIONED BY (complaint_year)
    """

    spark.sql(sql_silver_table_create)


def promote_to_silver(spark, bronze_table, silver_table):

    # Create a select query to gather records for loading by year
    sql_query = f"""SELECT year(date(date_received)) complaint_year, count(1) count 
    FROM {bronze_table}
    WHERE 
    CAST(date_received AS DATE) IS NOT NULL AND
    (CAST(date_received AS DATE) IS NOT NULL OR date_sent_to_company IS NULL)
    GROUP BY complaint_year
    ORDER BY complaint_year ASC"""

    # Create a list of objects containing year and count
    list_of_rows = [row.asDict() for row in spark.sql(sql_query).collect()]

    # Print a count of records by year
    print("Year" + "\t" + "Count")
    print("----" + "\t" + "-----")
    for record in list_of_rows:
        print(str(record["complaint_year"]) + "\t" + str(record["count"]))

    # Create a template SQL to promote records to silver
    silver_promotion_sql = """SELECT year(date(date_received)) as complaint_year, CAST(date_received AS DATE), product, sub_product, issue, sub_issue, complaint_what_happened, 
    company_public_response, company, state, zip_code, tags, consumer_consent_provided, submitted_via, CAST(date_sent_to_company AS DATE), company_response, timely,
    consumer_disputed, complaint_id, data_source, creation_time FROM {bronze_table} 
    WHERE 
    year(date(date_received)) = '{insert_year}' AND
    CAST(date_received AS DATE) IS NOT NULL AND
    (CAST(date_received AS DATE) IS NOT NULL OR date_sent_to_company IS NULL)
    ORDER BY CAST(date_received AS DATE)"""

    # For each year process the rows from bronze to silver
    for record in list_of_rows:

        print("Processing records for year: " + str(record["complaint_year"]))

        # Configure the table and ingest year
        template_values = {
            "bronze_table": bronze_table,
            "insert_year": str(record["complaint_year"]),
        }
        # Append records to the silver level table
        spark.sql(silver_promotion_sql.format(**template_values)).write.mode(
            "append"
        ).insertInto(silver_table)

    print()
    print("Completed")


def create_gold_table(spark, gold_table, table_format):

    print(f"Create the table {gold_table} if it does not exist")

    sql_gold_table_create = f"""CREATE TABLE IF NOT EXISTS {gold_table} (
    complaint_year STRING,
    date_received DATE,
    product STRING,
    sub_product STRING,
    issue STRING,
    sub_issue STRING,
    complaint_what_happened STRING,
    company_public_response STRING,
    company STRING,
    state STRING,
    zip_code STRING,
    tags STRING,
    consumer_consent_provided STRING,
    submitted_via STRING,
    date_sent_to_company DATE,
    company_response STRING,
    timely STRING,
    consumer_disputed STRING,
    complaint_id STRING,
    data_source STRING,
    creation_time TIMESTAMP)
    USING {table_format}
    PARTITIONED BY (complaint_year)
    """

    spark.sql(sql_gold_table_create)


def promote_to_gold(spark, silver_table, gold_table):

    # Create a select query to gather records for loading by year
    sql_query = f"""SELECT complaint_year, count(1) count 
    FROM {silver_table}
    GROUP BY complaint_year
    ORDER BY complaint_year ASC"""

    # Create a list of objects containing year and count
    list_of_rows = [row.asDict() for row in spark.sql(sql_query).collect()]

    # Print a count of records by year
    print("Year" + "\t" + "Count")
    print("----" + "\t" + "-----")
    for record in list_of_rows:
        print(str(record["complaint_year"]) + "\t" + str(record["count"]))

    gold_promotion_sql = """SELECT complaint_year, date_received, product, sub_product, issue, sub_issue, complaint_what_happened, 
    company_public_response, company, state, zip_code, tags, consumer_consent_provided, submitted_via, date_sent_to_company, company_response, timely,
    consumer_disputed, complaint_id, data_source, creation_time FROM {silver_table} 
    WHERE 
    complaint_year = '{insert_year}' 
    ORDER BY date_received"""

    # For each year process the rows from bronze to silver
    for record in list_of_rows:

        print("Processing records for year: " + str(record["complaint_year"]))

        # Configure the table and ingest year
        template_values = {
            "silver_table": silver_table,
            "insert_year": str(record["complaint_year"]),
        }
        # Append records to the silver level table
        spark.sql(gold_promotion_sql.format(**template_values)).write.mode(
            "append"
        ).insertInto(gold_table)

    print()
    print("Completed")


def get_query_start_date(spark, bronze_table):
    # Execute a query against the bronze table to find the latest date_received
    # Set the max_date_received to the latest date_received
    max_date = f"""
        SELECT CAST(MAX(to_date(date_received)) AS STRING) as max_date
        FROM {bronze_table}
    """
    list_of_rows = [row.asDict() for row in spark.sql(max_date).collect()]
    max_date_received = list_of_rows[0]["max_date"]

    # Convert the max_date_received string to a datetime object
    format_pattern = "%Y-%m-%d"
    datetime_query = datetime.strptime(max_date_received, format_pattern)
    # Add one day to the query date, need to query for the next day
    query_date = datetime_query + timedelta(days=1)

    # Set the api_query_date and the current date
    api_start_date = query_date.date()
    today = datetime.now(UTC).date()

    print(f"API queries will be exectued for days between {api_start_date} and {today}")

    return query_date


def query_api_and_send_to_kafka(spark, config_data, query_date):

    cfpb_api_request_template = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_min={date_received_min}&date_received_max={date_received_max}&format=json"

    end_date = datetime.now(UTC).date()

    while True:

        # if the query date reaches today, then exit, the day needs to be complete before querying
        if query_date.date() >= end_date:
            break

        # Create a string for the query date
        query_date_string = query_date.strftime("%Y-%m-%d")
        query_date_string = f"{query_date:%Y-%m-%d}"

        print(f"Query date for API Call: {query_date_string}")

        # substitute date values
        cfpb_api_request = cfpb_api_request_template.replace(
            "{date_received_min}", query_date_string
        ).replace("{date_received_max}", query_date_string)

        # Query the API
        response = requests.get(f"{cfpb_api_request}")

        # Load the results as a JSON
        api_results = json.loads(response.content)

        print(f"API query returned {len(api_results)} records for {query_date_string}")

        kafka_options = config_data["kafka"]["kafka_options"]

        # If values are returned, send to kafka
        if len(api_results) > 0:

            json_records = []

            # Send each result to the Kafka topic
            for result in api_results:
                json_records.append(result["_source"])

            # Create a dataframe of the results
            df = spark.createDataFrame(json_records)

            # Create a kafka compliant value and send to kafka
            df.select(to_json(struct("*")).alias("value")).selectExpr(
                "CAST(value AS STRING)"
            ).write.format("kafka").option(
                "topic", config_data["kafka"]["topic"]
            ).options(**kafka_options).save()

        # Move to the next date
        query_date = query_date + timedelta(days=1)


def load_streaming_monitor(spark):
    class KafkaSinkMonitor(StreamingQueryListener):
        def onQueryStarted(self, event):
            print(f"Query started: {event.id}")

        def onQueryProgress(self, event):
            # Extract the number of rows written to the Kafka sink
            num_sent = event.progress.sink.numOutputRows
            query_name = event.progress.name
            print(
                f"Query [{query_name}]: Sent {num_sent} records to the lakehouse in this batch."
            )

        def onQueryTerminated(self, event):
            print(f"Query terminated: {event.id}")

    spark.streams.addListener(KafkaSinkMonitor())


def get_complaint_schema():

    complaint_schema = StructType(
        [
            StructField("date_received", StringType(), True),
            StructField("product", StringType(), True),
            StructField("sub_product", StringType(), True),
            StructField("issue", StringType(), True),
            StructField("sub_issue", StringType(), True),
            StructField("complaint_what_happened", StringType(), True),
            StructField("company_public_response", StringType(), True),
            StructField("company", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("consumer_consent_provided", StringType(), True),
            StructField("submitted_via", StringType(), True),
            StructField("date_sent_to_company", StringType(), True),
            StructField("company_response", StringType(), True),
            StructField("timely", StringType(), True),
            StructField("consumer_disputed", StringType(), True),
            StructField("complaint_id", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("creation_time", TimestampType(), True),
        ]
    )

    return complaint_schema


def configure_spark_read_stream(spark, config_data):

    kafka_options = config_data["kafka"]["kafka_options"]

    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("subscribe", config_data["kafka"]["topic"])
        .options(**kafka_options)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "earliest")
        .load()
    )

    return kafka_stream_df


def stream_to_bronze_table(bronze_table, kafka_stream_df, complaint_schema):

    script_dir = Path(__file__).parent.resolve()

    string_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_payload")

    parsed_df = (
        string_df.withColumn("data", from_json(col("json_payload"), complaint_schema))
        .select("data.*")
        .withColumn("data_source", lit("kafka"))
        .withColumn("creation_time", localtimestamp())
    )

    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{script_dir}/checkpoint_read")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )

    query.awaitTermination()


def show_creation_times(spark, table_name):

    print(f"Show creation timestamps in table: {table_name}")

    sql_table = f"""SELECT DISTINCT(creation_time) FROM {table_name} ORDER BY creation_time DESC"""
    spark.sql(sql_table).show(truncate=False)


def show_missing_timestamps(spark, base_table_name, target_table_name):

    print(
        f"Show creation timestamps in table {base_table_name} that do not exist in {target_table_name}"
    )

    sql_missing_timestamps = f"""SELECT DISTINCT(creation_time) FROM {base_table_name} b
        WHERE NOT EXISTS 
        (SELECT DISTINCT(creation_time) FROM {target_table_name} s WHERE b.creation_time = s.creation_time)
        ORDER BY creation_time DESC"""

    spark.sql(sql_missing_timestamps).show(truncate=False)


def merge_to_silver(spark, bronze_table, silver_table):

    sql_merge = f"""
    MERGE INTO {silver_table} AS target
    USING (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY complaint_id 
                ORDER BY creation_time DESC
            ) as rn
        FROM {bronze_table} B WHERE NOT EXISTS 
            (SELECT 1 FROM {silver_table} S WHERE S.creation_time = B.creation_time)
    ) 
    WHERE rn = 1 
    ) AS source
    ON target.complaint_id = source.complaint_id
    WHEN MATCHED AND source.creation_time <> target.creation_time THEN
    UPDATE SET 
        target.date_received = CAST(source.date_received AS DATE), 
        target.product = source.product, 
        target.sub_product = source.sub_product, 
        target.issue = source.issue,
        target.sub_issue = source.sub_issue, 
        target.complaint_what_happened = source.complaint_what_happened, 
        target.company_public_response = source.company_public_response,
        target.company = source.company, 
        target.state = source.state, 
        target.zip_code = source.zip_code, 
        target.tags = source.tags, 
        target.consumer_consent_provided = source.consumer_consent_provided,
        target.submitted_via = source.submitted_via, 
        target.date_sent_to_company = CAST(source.date_sent_to_company AS DATE),
        target.company_response = source.company_response, 
        target.timely = source.timely, 
        target.consumer_disputed = source.consumer_disputed,
        target.data_source = source.data_source, 
        target.creation_time = source.creation_time
    WHEN NOT MATCHED THEN
    INSERT (complaint_year, date_received, product, sub_product, 
                issue, sub_issue, complaint_what_happened, company_public_response, 
                company, state, zip_code, tags, consumer_consent_provided, 
                submitted_via, date_sent_to_company,
                company_response, timely, consumer_disputed, complaint_id,
                data_source, creation_time) 
    VALUES  (year(date(date_received)), CAST(date_received AS DATE), source.product, source.sub_product, 
                source.issue, source.sub_issue, source.complaint_what_happened, source.company_public_response,
                source.company, source.state, source.zip_code, source.tags, source.consumer_consent_provided,
                source.submitted_via, CAST(date_sent_to_company AS DATE),
                source.company_response, source.timely, source.consumer_disputed, source.complaint_id,
                source.data_source, source.creation_time)"""

    spark.sql(sql_merge).show()


def merge_to_gold(spark, silver_table, gold_table):

    sql_merge = f"""
    MERGE INTO {gold_table} AS target
    USING (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY complaint_id 
                ORDER BY creation_time DESC
            ) as rn
        FROM {silver_table} B WHERE NOT EXISTS 
            (SELECT 1 FROM {gold_table} S WHERE S.creation_time = B.creation_time)
    ) 
    WHERE rn = 1 
    ) AS source
    ON target.complaint_id = source.complaint_id
    WHEN MATCHED AND source.creation_time <> target.creation_time THEN
    UPDATE SET 
        target.date_received = CAST(source.date_received AS DATE), 
        target.product = source.product, 
        target.sub_product = source.sub_product, 
        target.issue = source.issue,
        target.sub_issue = source.sub_issue, 
        target.complaint_what_happened = source.complaint_what_happened, 
        target.company_public_response = source.company_public_response,
        target.company = source.company, 
        target.state = source.state, 
        target.zip_code = source.zip_code, 
        target.tags = source.tags, 
        target.consumer_consent_provided = source.consumer_consent_provided,
        target.submitted_via = source.submitted_via, 
        target.date_sent_to_company = CAST(source.date_sent_to_company AS DATE),
        target.company_response = source.company_response, 
        target.timely = source.timely, 
        target.consumer_disputed = source.consumer_disputed,
        target.data_source = source.data_source, 
        target.creation_time = source.creation_time 
    WHEN NOT MATCHED THEN
        INSERT (complaint_year, date_received, product, sub_product, 
            issue, sub_issue, complaint_what_happened, company_public_response, 
            company, state, zip_code, tags, consumer_consent_provided, 
            submitted_via, date_sent_to_company,
            company_response, timely, consumer_disputed, complaint_id,
            data_source, creation_time) 
        VALUES  (year(date(date_received)), CAST(date_received AS DATE), source.product, source.sub_product, 
            source.issue, source.sub_issue, source.complaint_what_happened, source.company_public_response,
            source.company, source.state, source.zip_code, source.tags, source.consumer_consent_provided,
            source.submitted_via, CAST(date_sent_to_company AS DATE),
            source.company_response, source.timely, source.consumer_disputed, source.complaint_id,
            source.data_source, source.creation_time)
    """

    spark.sql(sql_merge).show()


def create_dimensional_tables(spark, default_catalog, level, target_schema, source_table):

    company_table = f"{default_catalog}.{target_schema}.company"
    summary_by_state_table = f"{default_catalog}.{target_schema}.summary_by_state"
    summary_by_year_table = f"{default_catalog}.{target_schema}.summary_by_year"

    create_company_table(spark, company_table, "iceberg", source_table)

    retrieve_record_count(spark, company_table)

    create_state_summary_table(
        spark, summary_by_state_table, "iceberg", source_table
    )

    retrieve_record_count(spark, summary_by_state_table)

    create_summary_by_year_table(
        spark, summary_by_year_table, level, "iceberg", source_table
    )

    retrieve_record_count(spark, summary_by_year_table)

def inital_loading_bronze(spark, config_data, default_catalog):

    storage_config = config_data["storage"]
    medallion_config = config_data["medallion"]
    bucket_name = storage_config["bucket_name"]
    bronze_schema = medallion_config["bronze_schema"]
    read_location = storage_config["read_location"]

    bronze_table = f"{default_catalog}.{bronze_schema}.complaints"

    create_schema(spark, bucket_name, default_catalog, bronze_schema)

    ingest_complaint_data(spark, bronze_table, "iceberg", read_location)

    retrieve_record_count(spark, bronze_table)

    create_dimensional_tables(spark, default_catalog, "bronze", bronze_schema, bronze_table)

    print("Bronze Loading Completed")

def inital_loading_silver(spark, config_data, default_catalog):

    storage_config = config_data["storage"]
    medallion_config = config_data["medallion"]
    bucket_name = storage_config["bucket_name"]
    bronze_schema = medallion_config["bronze_schema"]
    silver_schema = medallion_config["silver_schema"]

    bronze_table = f"{default_catalog}.{bronze_schema}.complaints"
    silver_table = f"{default_catalog}.{silver_schema}.complaints"

    create_schema(spark, bucket_name, default_catalog, silver_schema)

    drop_table(spark, silver_table)

    create_silver_table(spark, silver_table, "iceberg")

    promote_to_silver(spark, bronze_table, silver_table)

    retrieve_record_count(spark, silver_table)

    create_dimensional_tables(spark, default_catalog, "silver", silver_schema, silver_table)

    print("Silver Loading Completed")

def inital_loading_gold(spark, config_data, default_catalog):

    storage_config = config_data["storage"]
    medallion_config = config_data["medallion"]
    bucket_name = storage_config["bucket_name"]
    silver_schema = medallion_config["silver_schema"]
    gold_schema = medallion_config["gold_schema"]

    silver_table = f"{default_catalog}.{silver_schema}.complaints"
    gold_table = f"{default_catalog}.{gold_schema}.complaints"

    create_schema(spark, bucket_name, default_catalog, gold_schema)

    drop_table(spark, gold_table)
    
    create_gold_table(spark, gold_table, "iceberg")

    promote_to_gold(spark, silver_table, gold_table)

    retrieve_record_count(spark, gold_table)

    create_dimensional_tables(spark, default_catalog, "gold", gold_schema, gold_table)

    print("Gold Loading Completed")


def send_to_topic(spark, config_data, default_catalog):

    bronze_schema = config_data["medallion"]["bronze_schema"]

    bronze_table = f"{default_catalog}.{bronze_schema}.complaints"

    query_start_date = get_query_start_date(spark, bronze_table)

    query_api_and_send_to_kafka(spark, config_data, query_start_date)

    print("Kafka Loading Completed")


def stream_to_bronze(spark, config_data, default_catalog):

    bronze_schema = config_data["medallion"]["bronze_schema"]

    bronze_table = f"{default_catalog}.{bronze_schema}.complaints"

    load_streaming_monitor(spark)

    kafka_stream_df = configure_spark_read_stream(spark, config_data)

    complaint_schema = get_complaint_schema()

    stream_to_bronze_table(bronze_table, kafka_stream_df, complaint_schema)

    create_dimensional_tables(spark, default_catalog, "bronze", bronze_schema, bronze_table)

    print("Kafka Stream to Bronze Completed")


def incremental_silver_process(spark, config_data, default_catalog):

    medallion_config = config_data["medallion"]
    bronze_schema = medallion_config["bronze_schema"]
    silver_schema = medallion_config["silver_schema"]

    bronze_table = f"{default_catalog}.{bronze_schema}.complaints"
    silver_table = f"{default_catalog}.{silver_schema}.complaints"

    show_creation_times(spark, bronze_table)

    show_missing_timestamps(spark, bronze_table, silver_table)

    merge_to_silver(spark, bronze_table, silver_table)

    create_dimensional_tables(spark, default_catalog, "silver", silver_schema, silver_table)

    print("Silver Incremental Processing Completed")

def incremental_gold_process(spark, config_data, default_catalog):

    config_data = read_config()

    medallion_config = config_data["medallion"]
    silver_schema = medallion_config["silver_schema"]
    gold_schema = medallion_config["gold_schema"]

    silver_table = f"{default_catalog}.{silver_schema}.complaints"
    gold_table = f"{default_catalog}.{gold_schema}.complaints"

    show_creation_times(spark, silver_table)

    show_missing_timestamps(spark, silver_table, gold_table)

    merge_to_gold(spark, silver_table, gold_table)

    create_dimensional_tables(spark, default_catalog, "gold", gold_schema, gold_table)

    print("Gold Incremental Processing Completed")