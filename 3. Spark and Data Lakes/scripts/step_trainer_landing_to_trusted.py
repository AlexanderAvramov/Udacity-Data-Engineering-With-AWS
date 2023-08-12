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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aaa/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1691831133399 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-aaa",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1691831133399",
)

# Script generated for node Privacy Filter Join
PrivacyFilterJoin_node1691831155630 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1691831133399,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="PrivacyFilterJoin_node1691831155630",
)

# Script generated for node Drop Fields
DropFields_node1691831219657 = DropFields.apply(
    frame=PrivacyFilterJoin_node1691831155630,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1691831219657",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1691831244357 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691831219657,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aaa/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1691831244357",
)

job.commit()
