from medallion import read_config, connect_to_spark, configure_storage


def validate_spark_connection(config_data):

    spark = connect_to_spark(config_data, "Validate connection", "spark_catalog")

    try:
        spark.sql("show databases")
        print("Spark Connection Successful")
        return spark
    except Exception as e:
        print("Spark Connection Failed")
        raise Exception("Spark Connection Failed")


def validate_source_bucket(spark, config_data):

    # Access values from the configuration file
    source_bucket = config_data["source_bucket"]

    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    # hadoopConf = configure_storage(spark, config_data)
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()

    s3_path = f"s3a://{source_bucket}/"
    fs = FileSystem.get(sc._gateway.jvm.java.net.URI(s3_path), hadoopConf)

    try:
        files = fs.listFiles(Path(s3_path), True)
        if files.hasNext():
            print("Source Bucket Connection Succeeded")
        else:
            print("Source Bucket Connection Failed")
            raise Exception("Source Bucket Connection Failed")
    except Exception as e:
        print("Source Bucket Connection Failed")
        raise Exception("Source Bucket Connection Failed")
    finally:
        fs.close()


def main():

    config_data = read_config()

    spark = validate_spark_connection(config_data)

    # validate_source_bucket(spark, config_data)

    spark.stop()

    print("Validation Complete")


if __name__ == "__main__":
    main()
