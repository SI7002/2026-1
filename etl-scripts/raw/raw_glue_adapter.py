import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

from raw_ingestion_core import run_raw_ingestion


def glue_postgres_reader(
    spark: SparkSession,
    connection_name: str,
    source_table: str,
    target_entity: str
) -> DataFrame:
    """
    Lector PostgreSQL usando la conexión administrada en Glue.
    """
    glue_context = GlueContext(spark.sparkContext)

    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": source_table
        },
        transformation_ctx=f"{target_entity}_source"
    )

    return source_dyf.toDF()


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "connection_name",
        "source_table",
        "target_entity",
        "s3_target_path"
    ]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

run_raw_ingestion(
    spark=spark,
    source_reader=glue_postgres_reader,
    connection_name=args["connection_name"],
    source_table=args["source_table"],
    target_entity=args["target_entity"],
    s3_target_path=args["s3_target_path"]
)

job.commit()