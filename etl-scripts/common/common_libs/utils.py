from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def normalize_books(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("book_id").cast("string").alias("book_id"),
            F.col("isbn").cast("string").alias("isbn"),
            F.col("title").cast("string").alias("title"),
            F.col("author").cast("string").alias("author"),
            F.col("category").cast("string").alias("category"),
            F.col("publisher").cast("string").alias("publisher"),
            F.col("publication_year").cast("int").alias("publication_year"),
            F.col("price").cast("decimal(10,2)").alias("price"),
            F.col("stock").cast("int").alias("stock"),
            F.col("description").cast("string").alias("description"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("deleted_at").cast("timestamp").alias("deleted_at")
        )
    )


def normalize_customers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.col("email").cast("string").alias("email"),
            F.col("phone").cast("string").alias("phone"),
            F.col("city").cast("string").alias("city"),
            F.col("country").cast("string").alias("country"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("deleted_at").cast("timestamp").alias("deleted_at")
        )
    )

def normalize_orders(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("order_status").cast("string").alias("order_status"),
            F.col("total_amount").cast("decimal(12,2)").alias("total_amount"),
            F.col("order_timestamp").cast("timestamp").alias("order_timestamp"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("deleted_at").cast("timestamp").alias("deleted_at")
        )
    )


def normalize_order_items(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("order_item_id").cast("string").alias("order_item_id"),
            F.col("order_id").cast("string").alias("order_id"),
            F.col("book_id").cast("string").alias("book_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("line_total").cast("decimal(12,2)").alias("line_total"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at")
        )
    )


def add_ingestion_metadata(df: DataFrame, entity_name: str) -> DataFrame:
    return (
        df.withColumn("source_table", F.lit(entity_name))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
    )


def reorder_columns(df: DataFrame, output_schema: StructType) -> DataFrame:
    ordered_columns = [field.name for field in output_schema.fields]
    return df.select(*ordered_columns)


def apply_entity_normalization(df: DataFrame, entity_name: str) -> DataFrame:
    normalizers = {
        "books": normalize_books,
        "customers": normalize_customers,
        "orders": normalize_orders,
        "order_items": normalize_order_items        
    }

    if entity_name not in normalizers:
        raise ValueError(f"No normalizer registered for entity: {entity_name}")

    return normalizers[entity_name](df)


def apply_minimum_quality_filters(df: DataFrame, entity_name: str) -> DataFrame:
    if entity_name == "books":
        return df.filter(
            F.col("book_id").isNotNull() &
            F.col("title").isNotNull() &
            F.col("author").isNotNull() &
            F.col("price").isNotNull() &
            F.col("stock").isNotNull() &
            F.col("created_at").isNotNull() &
            F.col("updated_at").isNotNull()
        )

    if entity_name == "customers":
        return df.filter(
            F.col("customer_id").isNotNull() &
            F.col("first_name").isNotNull() &
            F.col("last_name").isNotNull() &
            F.col("email").isNotNull() &
            F.col("created_at").isNotNull() &
            F.col("updated_at").isNotNull()
        )
    
    if entity_name == "orders":
        return df.filter(
            F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("order_status").isNotNull() &
            F.col("total_amount").isNotNull() &
            F.col("order_timestamp").isNotNull() &
            F.col("created_at").isNotNull() &
            F.col("updated_at").isNotNull()
        )

    if entity_name == "order_items":
        return df.filter(
            F.col("order_item_id").isNotNull() &
            F.col("order_id").isNotNull() &
            F.col("book_id").isNotNull() &
            F.col("quantity").isNotNull() &
            (F.col("quantity") > 0) &
            F.col("unit_price").isNotNull() &
            (F.col("unit_price") >= 0) &
            F.col("created_at").isNotNull() &
            F.col("updated_at").isNotNull()
        )

    raise ValueError(f"No quality filter registered for entity: {entity_name}")