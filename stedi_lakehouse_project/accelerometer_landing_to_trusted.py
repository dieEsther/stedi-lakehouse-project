import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
CustomerTrusted_node1787318479285 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1787318479285")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1787318375229 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1787318375229")

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1787318675744 = Join.apply(frame1=CustomerTrusted_node1787318479285, frame2=AccelerometerLanding_node1787318375229, keys1=["email"], keys2=["user"], transformation_ctx="CustomerPrivacyJoin_node1787318675744")

# Script generated for node Drop Fields
DropFields_node1787319954416 = DropFields.apply(frame=CustomerPrivacyJoin_node1787318675744, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1787319954416")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1787319954416, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787317780404", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1787318811167 = glueContext.getSink(path="s3://esther-udacity-21082026/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1787318811167")
AmazonS3_node1787318811167.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1787318811167.setFormat("json")
AmazonS3_node1787318811167.writeFrame(DropFields_node1787319954416)
job.commit()