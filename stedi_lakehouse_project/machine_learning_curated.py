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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1790930081098 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1790930081098")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1790930083788 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1790930083788")

# Script generated for node SQL Query
SqlQuery1019 = '''
SELECT
    step.sensorReadingTime,
    step.serialNumber,
    step.distanceFromObject,
    accel.user,
    accel.x,
    accel.y,
    accel.z
FROM step
INNER JOIN accel
    ON step.sensorReadingTime = accel.timestamp
'''
SQLQuery_node1790930149432 = sparkSqlQuery(glueContext, query = SqlQuery1019, mapping = {"step":step_trainer_trusted_node1790930081098, "accel":accelerometer_trusted_node1790930083788}, transformation_ctx = "SQLQuery_node1790930149432")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790930149432, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790930009182", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790930267918 = glueContext.getSink(path="s3://esther-udacity-21082026/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790930267918")
AmazonS3_node1790930267918.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1790930267918.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1790930267918.writeFrame(SQLQuery_node1790930149432)
job.commit()