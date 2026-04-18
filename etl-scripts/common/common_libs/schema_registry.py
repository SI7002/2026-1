from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    DateType
)


def get_output_schema(entity_name: str) -> StructType:
    schemas = {
        "books": StructType([
            StructField("book_id", StringType(), False),
            StructField("isbn", StringType(), True),
            StructField("title", StringType(), False),
            StructField("author", StringType(), False),
            StructField("category", StringType(), True),
            StructField("publisher", StringType(), True),
            StructField("publication_year", IntegerType(), True),
            StructField("price", DecimalType(10, 2), False),
            StructField("stock", IntegerType(), False),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("deleted_at", TimestampType(), True),
            StructField("source_table", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False)
        ]),
        "customers": StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("phone", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("deleted_at", TimestampType(), True),
            StructField("source_table", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False)
        ]),
        "orders": StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("total_amount", DecimalType(12, 2), False),
            StructField("order_timestamp", TimestampType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("deleted_at", TimestampType(), True),
            StructField("source_table", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False)
        ]),
        "order_items": StructType([
            StructField("order_item_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("book_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("line_total", DecimalType(12, 2), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("source_table", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False)
        ])        
    }

    if entity_name not in schemas:
        raise ValueError(f"No output schema registered for entity: {entity_name}")

    return schemas[entity_name]