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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-aaa",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1691832783832 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-aaa",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1691832783832",
)

# Script generated for node Join
Join_node1691832900984 = Join.apply(
    frame1=StepTrainerTrusted_node1691832783832,
    frame2=AccelerometerTrusted_node1,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1691832900984",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1691832938647 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1691832900984,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aaa/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1691832938647",
)

job.commit()
