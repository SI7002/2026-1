from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate(df: DataFrame, pk_cols: List[str]) -> DataFrame:
    """
    Conserva la última versión por clave primaria lógica,
    usando updated_at como criterio de vigencia.
    """
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("updated_at").desc())

    return (
        df.withColumn("row_num", F.row_number().over(window_spec))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )


def apply_entity_rules(df: DataFrame, entity: str) -> DataFrame:
    """
    Aplica reglas mínimas de calidad y limpieza por entidad.
    """
    if entity == "books":
        return (
            df.filter(
                F.col("book_id").isNotNull() &
                F.col("isbn").isNotNull() &
                F.col("title").isNotNull() &
                F.col("author").isNotNull() &
                F.col("price").isNotNull() &
                (F.col("price") > 0) &
                F.col("stock").isNotNull() &
                (F.col("stock") >= 0)
            )
        )

    if entity == "customers":
        return (
            df.withColumn("email", F.lower(F.trim(F.col("email"))))
              .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
              .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
              .withColumn("city", F.initcap(F.trim(F.col("city"))))
              .withColumn("country", F.initcap(F.trim(F.col("country"))))
              .filter(
                  F.col("customer_id").isNotNull() &
                  F.col("email").isNotNull() &
                  (F.length(F.col("email")) > 0)
              )
        )

    if entity == "orders":
        return (
            df.filter(
                F.col("order_id").isNotNull() &
                F.col("customer_id").isNotNull() &
                F.col("order_timestamp").isNotNull() &
                F.col("total_amount").isNotNull() &
                (F.col("total_amount") >= 0) &
                F.col("order_status").isin("paid", "completed", "shipped")
            )
        )

    if entity == "order_items":
        return (
            df.filter(
                F.col("order_id").isNotNull() &
                F.col("book_id").isNotNull() &
                F.col("quantity").isNotNull() &
                (F.col("quantity") > 0) &
                F.col("unit_price").isNotNull() &
                (F.col("unit_price") > 0)
            )
        )

    raise ValueError(f"Unsupported entity for curated process: {entity}")


def add_curated_metadata(df: DataFrame, entity: str) -> DataFrame:
    """
    Agrega metadata mínima de publicación curated.
    """
    return (
        df.withColumn("curated_entity", F.lit(entity))
          .withColumn("curated_timestamp", F.current_timestamp())
          .withColumn("curated_date", F.to_date(F.current_timestamp()))
    )


def build_curated(df: DataFrame, entity: str, pk_map: Dict[str, List[str]]) -> DataFrame:
    """
    Pipeline principal de transformación hacia curated.
    """
    if entity not in pk_map:
        raise ValueError(f"No PK mapping configured for entity: {entity}")

    pk_cols = pk_map[entity]

    curated_df = deduplicate(df, pk_cols)
    curated_df = apply_entity_rules(curated_df, entity)
    curated_df = add_curated_metadata(curated_df, entity)

    return curated_df