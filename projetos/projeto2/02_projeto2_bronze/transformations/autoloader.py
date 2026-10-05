_from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col


@dp.table(
    name="projeto2_bronze",
    comment="Dados brutos"
)
def cnpj_bronze():

    source_path = "/Volumes/projeto2/bronze/raw_csv"
    checkpoint_path = "/Volumes/projeto2/bronze/checkpoint"
    schema_path = "/Volumes/projeto2/bronze/schema"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .load(source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
    )
