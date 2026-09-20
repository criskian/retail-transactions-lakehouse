"""Gold: data marts analíticos que alimentan el dashboard.

Dos familias de tablas:

* **Marts de negocio** — KPIs, features de cliente/producto, métricas por
  categoría. Alimentan los modelos y los paneles agregados.
* **Marts de serving** (`fact_*_daily`, `dim_customer_top_products`) — existen
  para que el dashboard **nunca lea Silver**. Silver pesa 379 MB; estos
  agregados pesan ~17 MB y permiten las mismas consultas filtradas por fecha y
  tienda. Es lo que hace desplegable la aplicación y lo que el documento de
  arquitectura ya prometía ("el dashboard nunca lee Silver directamente").
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .paths import GOLD, SILVER
from .spark_session import get_spark
from .storage import cleanup_temp_dirs, write_parquet_atomic


def _business_marts(items) -> dict:
    """Marts agregados por dimensión de negocio."""
    fact_sales_daily = (
        items.groupBy("date", "store_id")
        .agg(
            F.sum("qty").alias("units"),
            F.countDistinct("transaction_id").alias("txn_count"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .orderBy("date", "store_id")
    )

    max_date = items.agg(F.max("date")).first()[0]
    dim_customer = (
        items.groupBy("customer_id")
        .agg(
            F.countDistinct("transaction_id").alias("frequency"),
            F.sum("qty").alias("units_total"),
            F.countDistinct("product_id").alias("distinct_products"),
            F.countDistinct("category_id").alias("distinct_categories"),
            F.max("date").alias("last_purchase_date"),
        )
        .withColumn("avg_basket_size", (F.col("units_total") / F.col("frequency")).cast("double"))
        .withColumn("recency_days", F.datediff(F.lit(max_date), F.col("last_purchase_date")))
        .select(
            "customer_id",
            "frequency",
            "units_total",
            "distinct_products",
            "distinct_categories",
            "avg_basket_size",
            "recency_days",
        )
    )

    dim_product = items.groupBy("product_id", "category_id", "category_name").agg(
        F.sum("qty").alias("units_total"),
        F.countDistinct("transaction_id").alias("txn_count"),
        F.countDistinct("customer_id").alias("distinct_customers"),
    )

    cat_metrics = items.groupBy("category_id", "category_name").agg(
        F.sum("qty").alias("units"),
        F.countDistinct("transaction_id").alias("txn_count"),
        F.countDistinct("customer_id").alias("customers"),
        F.countDistinct("product_id").alias("distinct_products"),
    )

    kpis = items.agg(
        F.sum("qty").alias("total_units"),
        F.countDistinct("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.countDistinct("product_id").alias("total_products"),
        # Las filas sin categoría no cuentan como categoría observada.
        F.countDistinct(F.when(F.col("category_id").isNotNull(), F.col("category_id"))).alias(
            "total_categories"
        ),
        F.countDistinct("store_id").alias("total_stores"),
        F.min("date").alias("date_min"),
        F.max("date").alias("date_max"),
    )

    return {
        "fact_sales_daily": (fact_sales_daily, None, None),
        "dim_customer_features": (dim_customer, None, None),
        "dim_product_features": (dim_product, None, 1),
        "fact_category_metrics": (cat_metrics, None, 1),
        "fact_kpis": (kpis, None, 1),
    }


def _serving_marts(items) -> dict:
    """Agregados que sustituyen las consultas del dashboard contra Silver."""
    # Top-N de productos filtrable por fecha y tienda.
    fact_product_daily = items.groupBy("date", "store_id", "product_id").agg(
        F.sum("qty").alias("units"),
        F.countDistinct("transaction_id").alias("txns"),
        F.countDistinct("customer_id").alias("customers"),
    )

    # Panel de categorías filtrable.
    fact_category_daily = items.groupBy("date", "store_id", "category_id").agg(
        F.sum("qty").alias("units"),
        F.countDistinct("transaction_id").alias("txns"),
        F.countDistinct("customer_id").alias("customers"),
    )

    # Top-N de clientes, boxplots y conteo de clientes únicos filtrados.
    fact_customer_daily = items.groupBy("date", "store_id", "customer_id").agg(
        F.sum("qty").alias("units"),
        F.countDistinct("transaction_id").alias("txns"),
        F.countDistinct("product_id").alias("products"),
    )

    # Historial mostrado junto a las recomendaciones ALS.
    top_products_window = Window.partitionBy("customer_id").orderBy(
        F.col("units").desc(), F.col("product_id")
    )
    dim_customer_top_products = (
        items.groupBy("customer_id", "product_id")
        .agg(F.sum("qty").alias("units"), F.countDistinct("transaction_id").alias("txns"))
        .withColumn("rank", F.row_number().over(top_products_window))
        .filter(F.col("rank") <= 10)
    )

    return {
        "fact_product_daily": (fact_product_daily, None, 2),
        "fact_category_daily": (fact_category_daily, None, 1),
        "fact_customer_daily": (fact_customer_daily, None, 4),
        "dim_customer_top_products": (dim_customer_top_products, None, 4),
    }


def run():
    spark = get_spark("gold")
    try:
        cleanup_temp_dirs(GOLD)
        items = spark.read.parquet(str(SILVER / "transactions_items")).cache()

        marts = {**_business_marts(items), **_serving_marts(items)}
        for name, (df, partition_by, coalesce) in marts.items():
            write_parquet_atomic(df, GOLD / name, partition_by=partition_by, coalesce=coalesce)
            print(f"[gold] {name:28s} {spark.read.parquet(str(GOLD / name)).count():>10,} filas")

        items.unpersist()
        print("[gold] todos los marts escritos")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
