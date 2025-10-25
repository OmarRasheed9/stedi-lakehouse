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

# Script generated for node Accelerometer
Accelerometer_node1761309779707 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_landing", transformation_ctx="Accelerometer_node1761309779707")

# Script generated for node Customer trusted
Customertrusted_node1761309818855 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="Customertrusted_node1761309818855")

# Script generated for node SQL Query
SqlQuery179 = '''
select Accelerometer.* from Accelerometer 
JOIN Customer_trusted 
ON Accelerometer.user = Customer_trusted.email
'''
SQLQuery_node1761309783162 = sparkSqlQuery(glueContext, query = SqlQuery179, mapping = {"Accelerometer":Accelerometer_node1761309779707, "Customer_trusted":Customertrusted_node1761309818855}, transformation_ctx = "SQLQuery_node1761309783162")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1761309783162, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761308791558", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1761309786695 = glueContext.getSink(path="s3://lakehouse-stedi-human-balance-analytics/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1761309786695")
AmazonS3_node1761309786695.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1761309786695.setFormat("json")
AmazonS3_node1761309786695.writeFrame(SQLQuery_node1761309783162)
job.commit()