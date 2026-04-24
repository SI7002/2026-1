from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_customer_features(
    customers_df: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    books_df: DataFrame,
    snapshot_date: str,
    churn_window_days: int = 60
) -> DataFrame:
    """
    Construye la tabla features/customer_features para predicción de churn.

    Unidad de análisis:
    - Una fila por cliente.
    - Solo clientes que han comprado al menos una vez.

    Definición de churn:
    - churn_label_60d = 1 si el cliente no ha comprado en más de 60 días
      respecto a snapshot_date.
    """

    snapshot = F.to_date(F.lit(snapshot_date))

    valid_customers = (
        customers_df
        .filter(F.col("deleted_at").isNull())
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("city").cast("string").alias("city"),
            F.col("country").cast("string").alias("country"),
            F.col("created_at").cast("timestamp").alias("customer_created_at")
        )
    )

    valid_orders = (
        orders_df
        .filter(F.col("deleted_at").isNull())
        .filter(F.col("order_status").isin("paid", "completed", "shipped"))
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("order_timestamp").cast("timestamp").alias("order_timestamp"),
            F.col("total_amount").cast("decimal(12,2)").alias("total_amount")
        )
    )

    order_agg = (
        valid_orders
        .groupBy("customer_id")
        .agg(
            F.min("order_timestamp").alias("first_order_timestamp"),
            F.max("order_timestamp").alias("last_order_timestamp"),
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spent"),
            F.avg("total_amount").alias("avg_order_value")
        )
    )

    order_items_clean = (
        order_items_df
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("book_id").cast("string").alias("book_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price")
        )
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price").isNotNull())
        .filter(F.col("unit_price") > 0)
    )

    books_clean = (
        books_df
        .filter(F.col("deleted_at").isNull())
        .select(
            F.col("book_id").cast("string").alias("book_id"),
            F.col("category").cast("string").alias("category")
        )
    )

    order_items_enriched = (
        order_items_clean
        .join(
            valid_orders.select("order_id", "customer_id"),
            on="order_id",
            how="inner"
        )
        .join(
            books_clean,
            on="book_id",
            how="left"
        )
    )

    books_agg = (
        order_items_enriched
        .groupBy("customer_id")
        .agg(
            F.countDistinct("book_id").alias("distinct_books_purchased"),
            F.sum("quantity").alias("total_items_purchased")
        )
    )

    category_counts = (
        order_items_enriched
        .filter(F.col("category").isNotNull())
        .groupBy("customer_id", "category")
        .agg(
            F.sum("quantity").alias("category_quantity")
        )
    )

    preferred_category_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("category_quantity").desc(),
            F.col("category").asc()
        )
    )

    preferred_category = (
        category_counts
        .withColumn("category_rank", F.row_number().over(preferred_category_window))
        .filter(F.col("category_rank") == 1)
        .select(
            "customer_id",
            F.col("category").alias("preferred_category")
        )
    )

    customer_features = (
        valid_customers
        .join(order_agg, on="customer_id", how="inner")
        .join(books_agg, on="customer_id", how="left")
        .join(preferred_category, on="customer_id", how="left")
        .withColumn("snapshot_date", snapshot)
        .withColumn("first_order_date", F.to_date("first_order_timestamp"))
        .withColumn("last_order_date", F.to_date("last_order_timestamp"))
        .withColumn(
            "days_since_last_order",
            F.datediff(F.col("snapshot_date"), F.col("last_order_date"))
        )
        .withColumn(
            "days_as_customer",
            F.datediff(F.col("snapshot_date"), F.to_date("customer_created_at"))
        )
        .withColumn(
            "churn_label_60d",
            F.when(
                F.col("days_since_last_order") > F.lit(churn_window_days),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn("feature_timestamp", F.current_timestamp())
        .fillna({
            "distinct_books_purchased": 0,
            "total_items_purchased": 0
        })
        .select(
            "customer_id",
            "snapshot_date",
            "city",
            "country",
            "first_order_date",
            "last_order_date",
            "days_since_last_order",
            "days_as_customer",
            "total_orders",
            "total_spent",
            "avg_order_value",
            "distinct_books_purchased",
            "total_items_purchased",
            "preferred_category",
            "churn_label_60d",
            "feature_timestamp"
        )
    )

    return customer_features