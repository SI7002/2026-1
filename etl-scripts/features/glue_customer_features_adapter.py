import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from customer_features_core import build_customer_features


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "curated_base_path",
        "features_base_path",
        "snapshot_date"
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


curated_base_path = args["curated_base_path"].rstrip("/")
features_base_path = args["features_base_path"].rstrip("/")
snapshot_date = args["snapshot_date"]

customers_path = f"{curated_base_path}/customers/"
orders_path = f"{curated_base_path}/orders/"
order_items_path = f"{curated_base_path}/order_items/"
books_path = f"{curated_base_path}/books/"

target_path = f"{features_base_path}/customer_features/"


print(f"Reading curated customers from: {customers_path}")
customers_df = spark.read.parquet(customers_path)

print(f"Reading curated orders from: {orders_path}")
orders_df = spark.read.parquet(orders_path)

print(f"Reading curated order_items from: {order_items_path}")
order_items_df = spark.read.parquet(order_items_path)

print(f"Reading curated books from: {books_path}")
books_df = spark.read.parquet(books_path)

print(f"Building customer_features for snapshot_date={snapshot_date}")
customer_features_df = build_customer_features(
    customers_df=customers_df,
    orders_df=orders_df,
    order_items_df=order_items_df,
    books_df=books_df,
    snapshot_date=snapshot_date,
    churn_window_days=60
)

print(f"Writing customer_features to: {target_path}")
(
    customer_features_df
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("snapshot_date")
    .save(target_path)
)

job.commit()