from typing import Callable

from pyspark.sql import DataFrame, SparkSession

from common_libs.schema_registry import get_output_schema
from common_libs.utils import (
    apply_entity_normalization,
    add_ingestion_metadata,
    reorder_columns,
    apply_minimum_quality_filters
)


def run_raw_ingestion(
    spark: SparkSession,
    source_reader: Callable[[SparkSession, str, str, str], DataFrame],
    connection_name: str,
    source_table: str,
    target_entity: str,
    s3_target_path: str
) -> None:
    """
    Ejecuta la ingesta hacia zona raw usando un lector inyectado.
    """
    output_schema = get_output_schema(target_entity)

    source_df = source_reader(
        spark,
        connection_name,
        source_table,
        target_entity
    )

    normalized_df = apply_entity_normalization(source_df, target_entity)
    enriched_df = add_ingestion_metadata(normalized_df, target_entity)
    ordered_df = reorder_columns(enriched_df, output_schema)
    final_df = apply_minimum_quality_filters(ordered_df, target_entity)

    (
        final_df.write
        .mode("append")
        .format("parquet")
        .partitionBy("ingestion_date")
        .save(f"{s3_target_path.rstrip('/')}/{target_entity}/")
    )