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

from medallion import (
    read_config,
    connect_to_spark,
    list_databases,
    inital_loading_bronze
)

def main():

    spark = None

    try:

        config_data = read_config()

        default_catalog = config_data["medallion"]["default_catalog"]

        spark = connect_to_spark(
            config_data, "Medallion Initial Loading - Bronze", default_catalog
        )

        list_databases(spark)

        inital_loading_bronze(spark, config_data, default_catalog)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
