import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-aaa",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1691829279635 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-aaa",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1691829279635",
)

# Script generated for node Privacy Filter Join
PrivacyFilterJoin_node1691829294640 = Join.apply(
    frame1=AccelerometerLanding_node1691829279635,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFilterJoin_node1691829294640",
)

# Script generated for node Drop Fields
DropFields_node1691829340270 = DropFields.apply(
    frame=PrivacyFilterJoin_node1691829294640,
    paths=["user", "x", "y", "z"],
    transformation_ctx="DropFields_node1691829340270",
)

# Script generated for node Amazon S3
AmazonS3_node1691829373141 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691829340270,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aaa/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1691829373141",
)

job.commit()
