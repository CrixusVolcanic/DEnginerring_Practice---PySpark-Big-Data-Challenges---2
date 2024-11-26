from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import to_date, year, split, lpad, concat_ws, avg, sum, row_number

def main():
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.csv("resources/data.csv", header=True)

    date_columns = ['Order Date', 'Ship Date']
    int_columns = ['Row ID']
    double_columns = ['Sales', 'Discount', 'Profit', 'Quantity']

    for dc in date_columns:

        df = df.withColumn(f"Month_{dc}", lpad(split(df["Order Date"], '/')[0], 2, '0'))
        df = df.withColumn(f"Day_{dc}", lpad(split(df["Order Date"], '/')[1], 2, '0'))
        df = df.withColumn(f"Year_{dc}", split(df["Order Date"], '/')[2])
        df = df.withColumn(dc, concat_ws("/", df[f"Year_{dc}"], df[f"Month_{dc}"], df[f"Day_{dc}"]))
        df = df.withColumn(dc, to_date(df[dc], "yyyy/MM/dd"))
        df = df.drop(df[f"Year_{dc}"], df[f"Month_{dc}"], df[f"Day_{dc}"])

    for ic in int_columns:
        df = df.withColumn(ic, df[ic].cast(IntegerType()))

    for fc in double_columns:
        df = df.withColumn(fc, df[fc].cast(FloatType()))

    df = df.withColumn('Year', year(df["Order Date"]))
    df = df.withColumn('Profit Margin', (df["Sales"] - df["Quantity"] * 10) / df["Sales"])

    df_agg = df.groupby("Year", "Region", "Category").agg(
                                    avg("Sales").alias("Average Sales"),
                                    sum("Sales").alias("Total Sales"),
                                    avg("Profit Margin").alias("Average Profit Margin")
                                ).orderBy("Year", "Total Sales")

    #Top Region's Sales per Year
    df_top_region = df_agg.groupby("Year", "Region").sum("Total Sales").orderBy("Year", "Region")
    df_top_region = df_top_region.withColumn("row_number", row_number().over(Window.partitionBy("Year").orderBy("sum(Total Sales)")))
    df_top_region.filter("row_number = 1").show()

    #Best Margin per Category
    df_top_category = df_agg.groupby("Category").avg("Average Profit Margin")
    df_top_category.orderBy("avg(Average Profit Margin)", ascending = False).show(1)

    df_agg.write.mode("overwrite").format("parquet").partitionBy(["Year", "Region"]).save("results.parquet")



if __name__ == "__main__":
    main()