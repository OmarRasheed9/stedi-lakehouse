import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""


# NEW: read required curated inputs from Data Catalog
Customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted"
)

Accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-db",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometer_trusted"
)

# Script generated for node SQL Query - build CUSTOMERS_CURATED
SqlQuery0 = '''
SELECT DISTINCT c.*
FROM customer_trusted c
JOIN accelerometer_trusted a
  ON a.user = c.email
'''
SQLQueryDroppingUnTrustedCustromer_node1761307112419 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": Customer_trusted,
        "accelerometer_trusted": Accelerometer_trusted
    },
    transformation_ctx="SQLQueryDroppingUnTrustedCustromer_node1761307112419"
)

# Script generated for node Customer Trusted  -> now writes CUSTOMERS_CURATED (Curated zone)
EvaluateDataQuality().process_rows(
    frame=SQLQueryDroppingUnTrustedCustromer_node1761307112419,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761307102497", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

CustomerTrusted_node1761307115924 = glueContext.getSink(
    path="s3://lakehouse-stedi-human-balance-analytics/curated/customers/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1761307115924"
)
CustomerTrusted_node1761307115924.setCatalogInfo(
    catalogDatabase="stedi-db",
    catalogTableName="customers_curated"
)
CustomerTrusted_node1761307115924.setFormat("json")
CustomerTrusted_node1761307115924.writeFrame(SQLQueryDroppingUnTrustedCustromer_node1761307112419)

job.commit()
