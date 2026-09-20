"""Bronze: lee los CSV crudos y los persiste en Parquet sin transformación."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .paths import BRONZE, LANDING_PRODUCTS, LANDING_TX
from .spark_session import get_spark

_TX_SCHEMA = StructType(
    [
        StructField("date_raw", StringType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_list_raw", StringType(), True),
    ]
)


def _list_csvs(directory: Path, what: str) -> list[str]:
    """Lista CSVs con pathlib en vez del glob de Hadoop.

    El glob de Hadoop (`.csv("dir/*.csv")`) llama a NativeIO$Windows.access0, que
    sin `hadoop.dll` lanza UnsatisfiedLinkError dentro de un ForkJoinPool: la
    excepción se traga y el job se cuelga para siempre en lugar de fallar.
    Pasar la lista explícita de rutas evita ese camino por completo.
    """
    files = sorted(
        str(p)
        for p in directory.glob("*.csv")
        if not p.name.startswith(".")  # ignora restos de macOS (.DS_Store, ._foo.csv)
    )
    if not files:
        raise FileNotFoundError(
            f"No se encontraron CSVs de {what} en {directory}.\n"
            f"El dataset no está versionado: revisa el README para saber cómo poblar "
            f"data/landing/."
        )
    return files


def _transactions(spark):
    csv_files = _list_csvs(LANDING_TX, "transacciones")
    print(f"[bronze] archivos de transacciones: {[Path(f).name for f in csv_files]}")

    raw = (
        spark.read.option("sep", "|")
        .option("header", "false")
        .schema(_TX_SCHEMA)
        .csv(csv_files)
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "store_from_file",
            F.regexp_extract(F.col("source_file"), r"(\d+)_Tran\.csv", 1).cast("int"),
        )
        .withColumn("date", F.to_date("date_raw", "yyyy-MM-dd"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return raw.select(
        "date",
        F.coalesce("store_id", "store_from_file").alias("store_id"),
        "customer_id",
        "product_list_raw",
        "source_file",
        "ingest_ts",
    )


def _categories(spark):
    schema = StructType(
        [
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True),
        ]
    )
    return (
        spark.read.option("sep", "|")
        .option("header", "false")
        .schema(schema)
        .csv(str(LANDING_PRODUCTS / "Categories.csv"))
    )


def _product_category(spark):
    return (
        spark.read.option("sep", "|")
        .option("header", "true")
        .csv(str(LANDING_PRODUCTS / "ProductCategory.csv"))
        .selectExpr(
            "cast(`v.Code_pr` as int) as product_id", "cast(`v.code` as int) as category_id"
        )
    )


def run():
    spark = get_spark("bronze")
    try:
        tx = _transactions(spark)
        cats = _categories(spark)
        pc = _product_category(spark)

        (tx.write.mode("overwrite").partitionBy("store_id").parquet(str(BRONZE / "transactions")))
        (cats.write.mode("overwrite").parquet(str(BRONZE / "categories")))
        (pc.write.mode("overwrite").parquet(str(BRONZE / "product_category")))

        print(f"[bronze] transactions written: {tx.count():,} rows")
        print(f"[bronze] categories       : {cats.count():,} rows")
        print(f"[bronze] product_category : {pc.count():,} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
