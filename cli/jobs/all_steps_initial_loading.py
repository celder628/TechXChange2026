from medallion import (
    read_config,
    connect_to_spark,
    list_databases,
    inital_loading_bronze,
    inital_loading_silver,
    inital_loading_gold
)

import os

def main():

    spark = None

    try:

        config_data = read_config()

        default_catalog = config_data["medallion"]["default_catalog"]

        spark = connect_to_spark(
            config_data, "Medallion Initial Loading - All", default_catalog
        )

        list_databases(spark)

        inital_loading_bronze(spark, config_data, default_catalog)

        inital_loading_silver(spark, config_data, default_catalog)

        inital_loading_gold(spark, config_data, default_catalog)

        print("Initial Loading Completed")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
