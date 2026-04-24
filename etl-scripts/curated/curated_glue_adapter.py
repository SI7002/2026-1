import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from curated_core import build_curated


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_base_path",
        "target_base_path",
        "target_entity"
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


PK_MAP = {
    "books": ["book_id"],
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "book_id"]
}

entity = args["target_entity"]
source_base_path = args["source_base_path"].rstrip("/")
target_base_path = args["target_base_path"].rstrip("/")

source_path = f"{source_base_path}/{entity}/"
target_path = f"{target_base_path}/{entity}/"

print(f"Reading raw entity from: {source_path}")
raw_df = spark.read.parquet(source_path)

print(f"Building curated dataset for entity: {entity}")
curated_df = build_curated(raw_df, entity, PK_MAP)

print(f"Writing curated entity to: {target_path}")
(
    curated_df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("curated_date")
    .save(target_path)
)

job.commit()