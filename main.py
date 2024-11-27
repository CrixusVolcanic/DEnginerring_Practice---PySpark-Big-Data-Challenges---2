from libraries.main_pyspark import PySparkVersion
from libraries.main_spark_sql import SparkSqlVersion

def main():

    PySparkVersion().main()
    SparkSqlVersion().main()

if __name__=="__main__":
    main()