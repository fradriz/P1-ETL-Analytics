# coding: utf-8

# spark-submit
from pyspark.sql import SparkSession


def set_spark():
    """
    Creating the SparkSession
    :return: SparkSession
    """
    spark = SparkSession.builder.appName("PostgreSQL connection with PySpark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_input_data(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)


def load_data_to_postgres(df):
    print("CREATE TABLE IN POSTGRES and load DF !")
    db_name = "my_data_db"
    table_name = "aa_wdi_raw"
    url = f"jdbc:postgresql://localhost:5432/{db_name}"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(
        url=url,
        table=table_name,
        mode="overwrite",
        properties=properties)

    print("FINISHED Ingesting data in PostgreSQL")


def main():
    spark = set_spark()
    print("Reading the input data")
    base_path = "../data/input/"

    df = read_input_data(spark, path=base_path + "WDIData.csv.bz2").drop("_c67")
    df.show(2)
    # df.printSchema()

    try:
        load_data_to_postgres(df)
    except Exception as e:
        print(f"ERROR - Can't load the data to Postgres: {e}")


if __name__ == '__main__':
    """
    spark-submit --master local --jars ./postgresql-42.7.1.jar loader.py
    """
    main()
