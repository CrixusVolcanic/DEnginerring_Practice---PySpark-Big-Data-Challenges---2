from pyspark.sql import SparkSession
from libraries.utils import format_column, setup_logger
from libraries.queries import *
from libraries.config import INPUT_FILE, OUTPUT_PATH

class SparkSqlVersion:

    def __init__(self) -> None:
        self.default_logger = setup_logger(name="SparkSQL Version", log_file="sparksql.log")

    def main(self):

        self.default_logger.info("Process started")

        try:
            
            spark = SparkSession.builder.getOrCreate()

            df = spark.read.csv(INPUT_FILE, header=True)

            for c in df.columns:
                df = df.withColumnRenamed(c, format_column(c))

            df.createOrReplaceTempView("raw_data")

            self.default_logger.info(f"The file {INPUT_FILE} has been loaded")

            spark.sql(transformatted_data)
            
            columns_to_exclude = ['order_date', 'ship_date', 'row_id', 'sales', 'quantity', 'discount', 'profit']
            selected_columns = [col for col in df.columns if col not in columns_to_exclude]
            
            spark.sql(casted_data.replace('selected_columns', ",".join(selected_columns)))
            
            self.default_logger.info("Data transformations have been done")
            
            spark.sql(agg_data)
            
            spark.sql(top_region_sales).show()
            
            spark.sql(best_margin_category).show()
            
            self.default_logger.info("Data aggregations have been done")
            
            aggregated_df = spark.table("agg_data")
            aggregated_df.write.mode("overwrite").format("parquet").partitionBy(["year", "region"]).save(f"{OUTPUT_PATH}/results_sql.parquet")

            self.default_logger.info("The results have been exported in parquet format partitioned by Year/Region")

        except Exception as e:
            self.default_logger.error(f"Error: {e}")