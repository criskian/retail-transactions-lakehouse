"""Modelos: segmentación K-Means + recomendación (FP-Growth y ALS).

Marts producidos en Gold:

    cluster_assignments        (customer_id, cluster_id)
    cluster_profiles           (cluster_id, n_customers, medias y medianas)
    cluster_pca                (muestra proyectada a 2D + centroides)
    kmeans_search              (k, silhouette_mean, silhouette_std, n_samples)
    product_rules              (antecedent_ids, consequent, support, confidence, lift)
    customer_recommendations   (customer_id, product_id, score, rank)

Los modelos entrenados se persisten en `data/models/`.
"""

from __future__ import annotations

import os
import shutil

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.functions import vector_to_array
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .paths import DATA, GOLD, SILVER
from .spark_session import get_spark
from .storage import cleanup_temp_dirs, write_parquet_atomic


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


MODELS_DIR = DATA / "models"

CLUSTER_FEATURES = [
    "frequency",
    "units_total",
    "distinct_products",
    "distinct_categories",
    "avg_basket_size",
    "recency_days",
]

KMEANS_K_RANGE = tuple(int(k) for k in os.environ.get("KMEANS_K_RANGE", "3,4,5,6").split(","))
# La búsqueda de k se promedia sobre varias submuestras: con una sola muestra
# del 10% el ganador oscilaba entre k=4 y k=5 (silhouettes en 0,47–0,51), y
# bastaba añadir un 0,45% de datos para que cambiase. Promediar estabiliza la
# decisión y además permite reportar la dispersión. Las submuestras se toman
# por hash del id (ver `_deterministic_sample`), no con `DataFrame.sample`.
KMEANS_SEARCH_SAMPLES = _env_int("KMEANS_SEARCH_SAMPLES", 5)
KMEANS_SAMPLE_FRACTION = _env_float("KMEANS_SAMPLE_FRACTION", 0.10)
PCA_SAMPLE_SIZE = _env_int("PCA_SAMPLE_SIZE", 15_000)

# FP-Growth opera sobre ~1,1 M canastas. Un min_support bajo dispara una
# explosión combinatoria del FP-tree (OOM reproducible en un nodo único).
FP_MIN_SUPPORT = _env_float("FP_MIN_SUPPORT", 0.05)
FP_MIN_CONFIDENCE = _env_float("FP_MIN_CONFIDENCE", 0.30)
FP_MAX_BASKET_SIZE = 30
FP_TOP_N_PRODUCTS = _env_int("FP_TOP_N_PRODUCTS", 200)

ALS_RANK = 16
ALS_MAX_ITER = _env_int("ALS_MAX_ITER", 10)
ALS_REG_PARAM = 0.05
ALS_TOP_N = 10


def _save_model(model, name: str) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    path = MODELS_DIR / name
    shutil.rmtree(path, ignore_errors=True)
    model.write().overwrite().save(str(path))


def _silhouette(model: KMeansModel, df: DataFrame) -> float:
    evaluator = ClusteringEvaluator(
        predictionCol="cluster_id",
        featuresCol="features",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    return float(evaluator.evaluate(model.transform(df)))


# ---------------------------------------------------------------------------
# Segmentación de clientes
# ---------------------------------------------------------------------------
def _deterministic_sample(df: DataFrame, fraction: float, seed: int) -> DataFrame:
    """Submuestra reproducible, basada en el hash del id y no en el particionado.

    `DataFrame.sample(seed=...)` **no** es reproducible entre corridas si cambia
    la disposición física de los ficheros: se apoya en el orden de las filas
    dentro de cada partición. Se observó en la práctica — los mismos datos y la
    misma semilla daban silhouettes distintos tras regenerar Gold.

    Filtrar por `hash(customer_id, seed) % 100` depende sólo del valor de la
    clave, así que la muestra es la misma siempre.
    """
    cutoff = round(fraction * 100)
    bucket = F.abs(F.hash(F.col("customer_id"), F.lit(seed))) % 100
    return df.filter(bucket < F.lit(cutoff))


def _search_k(feats: DataFrame) -> tuple[int, list[dict]]:
    """Elige k promediando el silhouette sobre varias submuestras."""
    scores: dict[int, list[float]] = {k: [] for k in KMEANS_K_RANGE}

    for i in range(KMEANS_SEARCH_SAMPLES):
        sample = _deterministic_sample(feats, KMEANS_SAMPLE_FRACTION, 42 + i).cache()
        for k in KMEANS_K_RANGE:
            km = KMeans(
                k=k,
                seed=42,
                featuresCol="features",
                predictionCol="cluster_id",
                maxIter=30,
                tol=1e-4,
            )
            scores[k].append(_silhouette(km.fit(sample), sample))
        sample.unpersist()

    rows = []
    for k, values in scores.items():
        mean = sum(values) / len(values)
        var = sum((v - mean) ** 2 for v in values) / len(values)
        rows.append(
            {
                "k": int(k),
                "silhouette_mean": float(mean),
                "silhouette_std": float(var**0.5),
                "n_samples": len(values),
            }
        )
        print(f"[models]   k={k}  silhouette={mean:.4f} ± {var**0.5:.4f}")

    best = max(rows, key=lambda r: r["silhouette_mean"])
    return best["k"], rows


def run_kmeans(spark) -> None:
    print("[models] K-Means → leyendo dim_customer_features ...")
    customers = spark.read.parquet(str(GOLD / "dim_customer_features"))

    assembler = VectorAssembler(inputCols=CLUSTER_FEATURES, outputCol="features_raw")
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features", withMean=True, withStd=True
    )
    prep = Pipeline(stages=[assembler, scaler]).fit(customers)
    feats = prep.transform(customers).select("customer_id", "features", *CLUSTER_FEATURES).cache()

    k_best, search_rows = _search_k(feats)
    print(f"[models] k seleccionado = {k_best}")

    final = KMeans(
        k=k_best,
        seed=42,
        featuresCol="features",
        predictionCol="cluster_id",
        maxIter=50,
        tol=1e-4,
    ).fit(feats)
    assignments = final.transform(feats).select(
        "customer_id", "cluster_id", "features", *CLUSTER_FEATURES
    )

    # Re-numeramos por tamaño descendente: los ids nativos de K-Means son
    # arbitrarios y cambiarían entre reentrenamientos, desestabilizando la UI.
    sizes = assignments.groupBy("cluster_id").count().orderBy(F.col("count").desc()).collect()
    remap = {row["cluster_id"]: i for i, row in enumerate(sizes)}
    remap_expr = F.create_map([F.lit(x) for kv in remap.items() for x in kv])
    assignments = assignments.withColumn("cluster_id", remap_expr[F.col("cluster_id")]).cache()

    write_parquet_atomic(
        assignments.select("customer_id", "cluster_id"),
        GOLD / "cluster_assignments",
        coalesce=4,
    )

    profiles = (
        assignments.groupBy("cluster_id")
        .agg(
            F.count("*").alias("n_customers"),
            *[F.avg(F.col(c)).alias(f"avg_{c}") for c in CLUSTER_FEATURES],
            *[
                F.expr(f"percentile_approx({c}, 0.5)").alias(f"median_{c}")
                for c in CLUSTER_FEATURES
            ],
        )
        .orderBy("cluster_id")
    )
    write_parquet_atomic(profiles, GOLD / "cluster_profiles", coalesce=1)

    search_df = spark.createDataFrame(search_rows).withColumn(
        "is_best", F.col("k") == F.lit(k_best)
    )
    write_parquet_atomic(search_df, GOLD / "kmeans_search", coalesce=1)

    _write_cluster_pca(spark, assignments, final, remap)

    _save_model(prep, "kmeans_preprocessor")
    _save_model(final, "kmeans_pipeline")
    assignments.unpersist()
    feats.unpersist()
    print(f"[models] segmentación escrita (k={k_best})")


def _write_cluster_pca(
    spark, assignments: DataFrame, model: KMeansModel, remap: dict[int, int]
) -> None:
    """Proyección 2D de los clusters — la 'visualización del clustering' del enunciado.

    Se persiste una muestra (el scatter de 131k puntos es ilegible y pesado) más
    los centroides proyectados con la misma matriz PCA.
    """
    pca = PCA(k=2, inputCol="features", outputCol="pc").fit(assignments)

    total = assignments.count()
    fraction = min(1.0, PCA_SAMPLE_SIZE / max(total, 1))
    sample = (
        pca.transform(_deterministic_sample(assignments, fraction, 42))
        .withColumn("_pc", vector_to_array("pc"))
        .select(
            "customer_id",
            "cluster_id",
            F.col("_pc")[0].alias("pc1"),
            F.col("_pc")[1].alias("pc2"),
            *CLUSTER_FEATURES,
        )
        .withColumn("kind", F.lit("point"))
    )

    # Los centroides vienen con los ids nativos de K-Means: hay que aplicarles
    # el mismo remapeo por tamaño que a las asignaciones o no coincidirán.
    components = pca.pc.toArray()
    centers = spark.createDataFrame(
        [
            (
                int(remap[i]),
                float(c.dot(components[:, 0])),
                float(c.dot(components[:, 1])),
            )
            for i, c in enumerate(model.clusterCenters())
        ],
        ["cluster_id", "pc1", "pc2"],
    ).select(
        F.lit(None).cast("int").alias("customer_id"),
        "cluster_id",
        "pc1",
        "pc2",
        *[F.lit(None).cast("double").alias(c) for c in CLUSTER_FEATURES],
        F.lit("centroid").alias("kind"),
    )

    explained = [float(x) for x in pca.explainedVariance.toArray()]
    out = sample.unionByName(centers).withColumn("explained_variance", F.lit(sum(explained)))
    write_parquet_atomic(out, GOLD / "cluster_pca", coalesce=1)
    print(f"[models] cluster_pca escrito (varianza explicada {sum(explained):.1%})")


# ---------------------------------------------------------------------------
# Reglas de asociación
# ---------------------------------------------------------------------------
def run_fpgrowth(spark) -> None:
    print("[models] FP-Growth → construyendo canastas ...")
    items = spark.read.parquet(str(SILVER / "transactions_items"))

    top_products = (
        items.groupBy("product_id")
        .agg(F.sum("qty").alias("units"))
        .orderBy(F.col("units").desc())
        .limit(FP_TOP_N_PRODUCTS)
        .select("product_id")
    )
    filtered = items.join(F.broadcast(top_products), "product_id", "inner")

    baskets = (
        filtered.groupBy("transaction_id")
        .agg(F.collect_set("product_id").alias("items"))
        .filter((F.size("items") >= 2) & (F.size("items") <= FP_MAX_BASKET_SIZE))
        .cache()
    )
    n_baskets = baskets.count()

    print(
        f"[models] FP-Growth: top-{FP_TOP_N_PRODUCTS} productos, "
        f"{n_baskets:,} canastas, min_support={FP_MIN_SUPPORT}, "
        f"min_confidence={FP_MIN_CONFIDENCE}"
    )

    model = FPGrowth(
        itemsCol="items", minSupport=FP_MIN_SUPPORT, minConfidence=FP_MIN_CONFIDENCE
    ).fit(baskets)

    rules = model.associationRules

    # NO explotar el antecedente. FP-Growth produce antecedentes multi-ítem: la
    # regla {3,16} -> {21} con conf=0,575 NO significa que 3 -> 21 tenga esa
    # confianza. El código anterior hacía explode() sobre el array y generaba
    # filas falsas: 327 filas para sólo 215 pares distintos, con el par 5 -> 10
    # repetido 11 veces con 11 confianzas contradictorias.
    # El consecuente sí es siempre unitario en MLlib.
    flat = (
        rules.withColumn("antecedent_ids", F.array_sort("antecedent"))
        .withColumn("antecedent_size", F.size("antecedent"))
        .withColumn("antecedent_label", F.concat_ws(" + ", F.col("antecedent_ids")))
        .withColumn("consequent_product_id", F.element_at("consequent", 1))
    )

    # `support` existe en las versiones recientes de MLlib; si no, se deriva de
    # los itemsets frecuentes: support(X ∪ Y) = freq(X ∪ Y) / N.
    if "support" not in rules.columns:
        freq = model.freqItemsets.select(
            F.array_sort("items").alias("_itemset"),
            (F.col("freq") / F.lit(n_baskets)).alias("support"),
        )
        flat = (
            flat.withColumn(
                "_itemset",
                F.array_sort(F.concat(F.col("antecedent_ids"), F.col("consequent"))),
            )
            .join(freq, "_itemset", "left")
            .drop("_itemset")
        )

    prod = spark.read.parquet(str(GOLD / "dim_product_features")).select(
        "product_id", "category_name"
    )
    enriched = (
        flat.join(
            prod.withColumnRenamed("product_id", "consequent_product_id").withColumnRenamed(
                "category_name", "consequent_category"
            ),
            "consequent_product_id",
            "left",
        )
        .select(
            "antecedent_ids",
            "antecedent_label",
            "antecedent_size",
            "consequent_product_id",
            "consequent_category",
            F.col("support").cast("double").alias("support"),
            F.col("confidence").cast("double").alias("confidence"),
            F.col("lift").cast("double").alias("lift"),
        )
        .orderBy(F.col("lift").desc())
    )

    write_parquet_atomic(enriched, GOLD / "product_rules", coalesce=1)

    n_rules = enriched.count()
    n_simple = enriched.filter(F.col("antecedent_size") == 1).count()
    print(f"[models] FP-Growth: {n_rules:,} reglas ({n_simple:,} con antecedente simple)")

    baskets.unpersist()
    _save_model(model, "fpgrowth")


# ---------------------------------------------------------------------------
# Recomendación cliente → producto
# ---------------------------------------------------------------------------
def run_als(spark) -> None:
    print("[models] ALS implicit → matriz cliente × producto ...")
    items = spark.read.parquet(str(SILVER / "transactions_items"))

    interactions = items.groupBy("customer_id", "product_id").agg(
        F.sum("qty").cast("double").alias("rating")
    )

    model = ALS(
        userCol="customer_id",
        itemCol="product_id",
        ratingCol="rating",
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG_PARAM,
        implicitPrefs=True,
        coldStartStrategy="drop",
        seed=42,
    ).fit(interactions)

    exploded = (
        model.recommendForAllUsers(ALS_TOP_N)
        .withColumn("rec", F.explode("recommendations"))
        .select(
            F.col("customer_id"),
            F.col("rec.product_id").alias("product_id"),
            F.col("rec.rating").alias("score"),
        )
    )
    ranked = exploded.withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("score").desc())),
    )

    write_parquet_atomic(ranked, GOLD / "customer_recommendations", coalesce=4)
    print(f"[models] ALS: top-{ALS_TOP_N} recomendaciones por cliente persistidas")

    _save_model(model, "als")


def run() -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("models")
    try:
        cleanup_temp_dirs(GOLD)
        run_kmeans(spark)
        run_fpgrowth(spark)
        run_als(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
