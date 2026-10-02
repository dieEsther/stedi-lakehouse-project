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

# Script generated for node Customer Trusted
CustomerTrusted_node1790499990117 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1790499990117")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1790500050228 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1790500050228")

# Script generated for node SQL Query
SqlQuery716 = '''
SELECT DISTINCT customer.*
FROM customer
INNER JOIN accelerometer
    ON customer.email = accelerometer.user
'''
SQLQuery_node1790500102030 = sparkSqlQuery(glueContext, query = SqlQuery716, mapping = {"customer":CustomerTrusted_node1790499990117, "accelerometer":AccelerometerTrusted_node1790500050228}, transformation_ctx = "SQLQuery_node1790500102030")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790500102030, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790499836850", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790500318762 = glueContext.getSink(path="s3://esther-udacity-21082026/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790500318762")
AmazonS3_node1790500318762.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1790500318762.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1790500318762.writeFrame(SQLQuery_node1790500102030)
job.commit()