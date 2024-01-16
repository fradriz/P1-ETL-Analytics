# coding: utf-8

# spark-submit
from pyspark.sql import SparkSession
from pyspark.sql import dataframe
import pyspark.sql.functions as F


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


def load_data_to_postgres(df, table_name):
    print(f"Creating table in Postgres and loading to {table_name}")
    db_name = "my_data_db"

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

    print(f"Done with {table_name}")


def pivot_df(df: dataframe) -> dataframe:
    """
    Receive a df to pivot the numeric columns
    Ej.
    +------------+-----------------+----------------+----------------+
    |Country Code|Indicator Code   |2000            |2001            |
    +------------+-----------------+----------------+----------------+
    |ARG         |EG.ELC.ACCS.ZS   |95.6804733276367|95.5110634674458|
    |ARG         |NY.ADJ.NNTY.PC.CD|6430.93331567539|6002.28904723084|
    +------------+-----------------+----------------+----------------+

    To:
    +------------+-----------------+----+----------------+
    |Country Code|Indicator Code   |year|value           |
    +------------+-----------------+----+----------------+
    |ARG         |EG.ELC.ACCS.ZS   |2000|95.6804733276367|
    |ARG         |EG.ELC.ACCS.ZS   |2001|95.5110634674458|
    |ARG         |NY.ADJ.NNTY.PC.CD|2000|6430.93331567539|
    |ARG         |NY.ADJ.NNTY.PC.CD|2001|6002.28904723084|
    +------------+-----------------+----+----------------+

    :param raw_df:
    :return:
    """

    def _map_content(df):
        columns = df.columns
        r = []
        for c in columns:
            # we want to keep only the columns with a number (year) in its name.
            if c.isnumeric():
                r.append(F.lit(str(c)))
                r.append(F.col(str(c)))
        return r

    col_names = [
        "Country Code",
        "Country Name",
        "Indicator Code",
        "Indicator Name",
    ]

    return (df
            .select(*col_names, F.create_map(*_map_content(df)).alias("values"))
            .select(*col_names, F.explode("values")).withColumnRenamed('key', 'year')
            )


def main():
    spark = set_spark()
    print("Reading the input data")
    base_path = "../data/input/"

    raw_df = read_input_data(spark, path=base_path + "WDIData.csv.bz2").drop("_c67")
    pivoted_df = pivot_df(raw_df)
    pivoted_df.show(3)

    try:
        load_data_to_postgres(raw_df, table_name="aa_wdi_raw")
        load_data_to_postgres(pivoted_df, table_name="aa_wdi_transformed")
    except Exception as e:
        print(f"ERROR - Can't load the data to Postgres: {e}")


if __name__ == '__main__':
    """
    Usage: spark-submit --master local --jars ./postgresql-42.7.1.jar loader.py
    """
    main()
